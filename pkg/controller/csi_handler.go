/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	crdv1 "github.com/kubernetes-csi/external-snapshotter/pkg/apis/volumesnapshot/v1alpha1"
	clientset "github.com/kubernetes-csi/external-snapshotter/pkg/client/clientset/versioned"
	"github.com/kubernetes-csi/external-snapshotter/pkg/connection"
	"k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1beta1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
)

// Handler is responsible for handling VolumeSnapshot events from informer.
type Handler interface {
	CreateSnapshotOperation(snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshot, error)
	CheckandUpdateSnapshotStatusOperation(snapshot *crdv1.VolumeSnapshot, content *crdv1.VolumeSnapshotContent) (*crdv1.VolumeSnapshot, error)
	DeleteSnapshotContentOperation(content *crdv1.VolumeSnapshotContent) error
	GetSnapshotStatus(content *crdv1.VolumeSnapshotContent) (*csi.SnapshotStatus, int64, error)
	BindandUpdateVolumeSnapshot(snapshotContent *crdv1.VolumeSnapshotContent, snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshot, error)
	GetClassFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshotClass, error)
}

// csiHandler is a handler that calls CSI to create/delete volume snapshot.
type csiHandler struct {
	clientset                       clientset.Interface
	client                          kubernetes.Interface
	snapshotterName                 string
	eventRecorder                   record.EventRecorder
	csiConnection                   connection.CSIConnection
	timeout                         time.Duration
	createSnapshotContentRetryCount int
	createSnapshotContentInterval   time.Duration
	snapshotNamePrefix              string
	snapshotNameUUIDLength          int
}

func NewCSIHandler(
	clientset clientset.Interface,
	client kubernetes.Interface,
	snapshotterName string,
	eventRecorder record.EventRecorder,
	csiConnection connection.CSIConnection,
	timeout time.Duration,
	createSnapshotContentRetryCount int,
	createSnapshotContentInterval time.Duration,
	snapshotNamePrefix string,
	snapshotNameUUIDLength int,
) Handler {
	return &csiHandler{
		clientset:       clientset,
		client:          client,
		snapshotterName: snapshotterName,
		eventRecorder:   eventRecorder,
		csiConnection:   csiConnection,
		timeout:         timeout,
		createSnapshotContentRetryCount: createSnapshotContentRetryCount,
		createSnapshotContentInterval:   createSnapshotContentInterval,
		snapshotNamePrefix:              snapshotNamePrefix,
		snapshotNameUUIDLength:          snapshotNameUUIDLength,
	}
}

func (handler *csiHandler) deleteSnapshot(content *crdv1.VolumeSnapshotContent) error {
	if content.Spec.CSI == nil {
		return fmt.Errorf("CSISnapshot not defined in spec")
	}
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	err := handler.csiConnection.DeleteSnapshot(ctx, content.Spec.CSI.SnapshotHandle)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot data %s: %q", content.Name, err)
	}

	return nil
}

func (handler *csiHandler) GetSnapshotStatus(content *crdv1.VolumeSnapshotContent) (*csi.SnapshotStatus, int64, error) {
	if content.Spec.CSI == nil {
		return nil, 0, fmt.Errorf("CSISnapshot not defined in spec")
	}
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	csiSnapshotStatus, timestamp, err := handler.csiConnection.GetSnapshotStatus(ctx, content.Spec.CSI.SnapshotHandle)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list snapshot data %s: %q", content.Name, err)
	}
	return csiSnapshotStatus, timestamp, nil
}

func (handler *csiHandler) CheckandUpdateSnapshotStatusOperation(snapshot *crdv1.VolumeSnapshot, content *crdv1.VolumeSnapshotContent) (*crdv1.VolumeSnapshot, error) {
	status, _, err := handler.GetSnapshotStatus(content)
	if err != nil {
		return nil, fmt.Errorf("failed to check snapshot status %s with error %v", snapshot.Name, err)
	}

	newSnapshot, err := handler.updateSnapshotStatus(snapshot, status, time.Now(), true)
	if err != nil {
		return nil, err
	}
	return newSnapshot, nil
}

func makeSnapshotName(prefix, snapshotUID string, snapshotNameUUIDLength int) (string, error) {
	// create persistent name based on a volumeNamePrefix and volumeNameUUIDLength
	// of PVC's UID
	if len(prefix) == 0 {
		return "", fmt.Errorf("Snapshot name prefix cannot be of length 0")
	}
	if len(snapshotUID) == 0 {
		return "", fmt.Errorf("Corrupted snapshot object, it is missing UID")
	}
	if snapshotNameUUIDLength == -1 {
		// Default behavior is to not truncate or remove dashes
		return fmt.Sprintf("%s-%s", prefix, snapshotUID), nil
	}
	return fmt.Sprintf("%s-%s", prefix, strings.Replace(snapshotUID, "-", "", -1)[0:snapshotNameUUIDLength]), nil
}

// The function goes through the whole snapshot creation process.
// 1. Trigger the snapshot through csi storage provider.
// 2. Update VolumeSnapshot status with creationtimestamp information
// 3. Create the VolumeSnapshotContent object with the snapshot id information.
// 4. Bind the VolumeSnapshot and VolumeSnapshotContent object
func (handler *csiHandler) CreateSnapshotOperation(snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshot, error) {
	glog.Infof("createSnapshot: Creating snapshot %s through the plugin ...", snapshotKey(snapshot))

	class, err := handler.GetClassFromVolumeSnapshot(snapshot)
	if err != nil {
		glog.Errorf("CreateSnapshotOperation failed to getClassFromVolumeSnapshot %s", err)
		return nil, err
	}
	volume, err := handler.getVolumeFromVolumeSnapshot(snapshot)
	if err != nil {
		glog.Errorf("CreateSnapshotOperation failed to get PersistentVolume object [%s]: Error: [%#v]", snapshot.Name, err)
		return nil, err
	}

	// Call CSI create snapshot
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	snapshotName, err := makeSnapshotName(handler.snapshotNamePrefix, string(snapshot.UID), handler.snapshotNameUUIDLength)
	if err != nil {
		return nil, err
	}
	driverName, snapshotID, timestamp, csiSnapshotStatus, err := handler.csiConnection.CreateSnapshot(ctx, snapshotName, snapshot, volume, class.Parameters)
	if err != nil {
		return nil, fmt.Errorf("Failed to take snapshot of the volume, %s: %q", volume.Name, err)
	}
	glog.Infof("Create snapshot driver %s, snapshotId %s, timestamp %d, csiSnapshotStatus %v", driverName, snapshotID, timestamp, csiSnapshotStatus)

	// Update snapshot status with timestamp
	newSnapshot, err := handler.updateSnapshotStatus(snapshot, csiSnapshotStatus, time.Unix(0, timestamp), false)
	if err != nil {
		return nil, err
	}

	// Create VolumeSnapshotContent in the database
	contentName := GetSnapshotContentNameForSnapshot(snapshot)
	volumeRef, err := ref.GetReference(scheme.Scheme, volume)

	snapshotContent := &crdv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name: contentName,
		},
		Spec: crdv1.VolumeSnapshotContentSpec{
			VolumeSnapshotRef: &v1.ObjectReference{
				Kind:       "VolumeSnapshot",
				Namespace:  snapshot.Namespace,
				Name:       snapshot.Name,
				UID:        snapshot.UID,
				APIVersion: "v1alpha1",
			},
			PersistentVolumeRef: volumeRef,
			VolumeSnapshotSource: crdv1.VolumeSnapshotSource{
				CSI: &crdv1.CSIVolumeSnapshotSource{
					Driver:         driverName,
					SnapshotHandle: snapshotID,
					CreatedAt:      timestamp,
				},
			},
		},
	}

	// Try to create the VolumeSnapshotContent object several times
	for i := 0; i < handler.createSnapshotContentRetryCount; i++ {
		glog.V(4).Infof("createSnapshot [%s]: trying to save volume snapshot data %s", snapshotKey(snapshot), snapshotContent.Name)
		if _, err = handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshotContents().Create(snapshotContent); err == nil || apierrs.IsAlreadyExists(err) {
			// Save succeeded.
			if err != nil {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q already exists, reusing", snapshotContent.Name, snapshotKey(snapshot))
				err = nil
			} else {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q saved", snapshotContent.Name, snapshotKey(snapshot))
			}
			break
		}
		// Save failed, try again after a while.
		glog.V(3).Infof("failed to save volume snapshot data %q for snapshot %q: %v", snapshotContent.Name, snapshotKey(snapshot), err)
		time.Sleep(handler.createSnapshotContentInterval)
	}

	if err != nil {
		// Save failed. Now we have a storage asset outside of Kubernetes,
		// but we don't have appropriate volumesnapshotdata object for it.
		// Emit some event here and try to delete the storage asset several
		// times.
		strerr := fmt.Sprintf("Error creating volume snapshot data object for snapshot %s: %v.", snapshotKey(snapshot), err)
		glog.Error(strerr)
		handler.eventRecorder.Event(newSnapshot, v1.EventTypeWarning, "CreateSnapshotContentFailed", strerr)
		return nil, err
	}

	// save succeeded, bind and update status for snapshot.
	result, err := handler.BindandUpdateVolumeSnapshot(snapshotContent, newSnapshot)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Delete a snapshot
// 1. Find the SnapshotContent corresponding to Snapshot
//   1a: Not found => finish (it's been deleted already)
// 2. Ask the backend to remove the snapshot device
// 3. Delete the SnapshotContent object
// 4. Remove the Snapshot from vsStore
// 5. Finish
func (handler *csiHandler) DeleteSnapshotContentOperation(content *crdv1.VolumeSnapshotContent) error {
	glog.V(4).Infof("deleteSnapshotOperation [%s] started", content.Name)

	err := handler.deleteSnapshot(content)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot %#v, err: %v", content.Name, err)
	}

	err = handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshotContents().Delete(content.Name, &metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete VolumeSnapshotContent %s from API server: %q", content.Name, err)
	}

	return nil
}

func (handler *csiHandler) BindandUpdateVolumeSnapshot(snapshotContent *crdv1.VolumeSnapshotContent, snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshot, error) {
	glog.V(4).Infof("bindandUpdateVolumeSnapshot for snapshot [%s]: snapshotContent [%s]", snapshot.Name, snapshotContent.Name)
	snapshotObj, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Get(snapshot.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error get snapshot %s from api server: %v", snapshotKey(snapshot), err)
	}

	// Copy the snapshot object before updating it
	snapshotCopy := snapshotObj.DeepCopy()

	if snapshotObj.Spec.SnapshotContentName == snapshotContent.Name {
		glog.Infof("bindVolumeSnapshotContentToVolumeSnapshot: VolumeSnapshot %s already bind to volumeSnapshotContent [%s]", snapshot.Name, snapshotContent.Name)
	} else {
		glog.Infof("bindVolumeSnapshotContentToVolumeSnapshot: before bind VolumeSnapshot %s to volumeSnapshotContent [%s]", snapshot.Name, snapshotContent.Name)
		snapshotCopy.Spec.SnapshotContentName = snapshotContent.Name
		updateSnapshot, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshot.Namespace).Update(snapshotCopy)
		if err != nil {
			glog.Infof("bindVolumeSnapshotContentToVolumeSnapshot: Error binding VolumeSnapshot %s to volumeSnapshotContent [%s]. Error [%#v]", snapshot.Name, snapshotContent.Name, err)
			return nil, fmt.Errorf("error updating snapshot object %s on the API server: %v", snapshotKey(updateSnapshot), err)
		}
		snapshotCopy = updateSnapshot
	}

	glog.V(5).Infof("bindandUpdateVolumeSnapshot for snapshot completed [%#v]", snapshotCopy)
	return snapshotCopy, nil
}

// UpdateSnapshotStatus converts snapshot status to crdv1.VolumeSnapshotCondition
func (handler *csiHandler) updateSnapshotStatus(snapshot *crdv1.VolumeSnapshot, csistatus *csi.SnapshotStatus, timestamp time.Time, bound bool) (*crdv1.VolumeSnapshot, error) {
	glog.V(4).Infof("updating VolumeSnapshot[]%s, set status %v, timestamp %v", snapshotKey(snapshot), csistatus, timestamp)
	status := snapshot.Status
	change := false
	timeAt := &metav1.Time{
		Time: timestamp,
	}

	snapshotClone := snapshot.DeepCopy()
	switch csistatus.Type {
	case csi.SnapshotStatus_READY:
		if bound {
			status.Bound = true
			handler.eventRecorder.Event(snapshotClone, v1.EventTypeNormal, "BoundSnapshot", fmt.Sprintf("The requested snapshot %s is created and bound to a VolumeSnapshotContent", snapshotKey(snapshotClone)))
			change = true
		}
		if status.CreatedAt == nil {
			status.CreatedAt = timeAt
			handler.eventRecorder.Event(snapshotClone, v1.EventTypeNormal, "CreateSnapshot", fmt.Sprintf("The requested snapshot %s is created, waiting to bind to a VolumeSnapshotContent", snapshotKey(snapshotClone)))
			change = true
		}
	case csi.SnapshotStatus_ERROR_UPLOADING:
		if status.Error == nil {
			status.Error = &storage.VolumeError{
				Time:    *timeAt,
				Message: "Failed to upload the snapshot",
			}
			change = true
		}
	case csi.SnapshotStatus_UPLOADING:
		if status.CreatedAt == nil {
			status.CreatedAt = timeAt
			change = true
		}
	}
	if change {
		snapshotClone.Status = status
		newSnapshotObj, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshotClone.Namespace).Update(snapshotClone)
		if err != nil {
			return nil, fmt.Errorf("error update status for volume snapshot %s: %s", snapshotKey(snapshot), err)
		} else {
			return newSnapshotObj, nil
		}
	}
	return snapshot, nil
}

// getVolumeFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (handler *csiHandler) getVolumeFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*v1.PersistentVolume, error) {
	pvc, err := handler.getClaimFromVolumeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	pvName := pvc.Spec.VolumeName
	pv, err := handler.client.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PV %s from the API server: %q", pvName, err)
	}

	glog.V(5).Infof("getVolumeFromVolumeSnapshot: snapshot [%s] PV name [%s]", snapshot.Name, pvName)

	return pv, nil
}

// getClassFromVolumeSnapshot is a helper function to get storage class from VolumeSnapshot.
func (handler *csiHandler) GetClassFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshotClass, error) {
	className := snapshot.Spec.VolumeSnapshotClassName
	glog.V(5).Infof("getClassFromVolumeSnapshot [%s]: VolumeSnapshotClassName [%s]", snapshot.Name, className)
	class, err := handler.clientset.VolumesnapshotV1alpha1().VolumeSnapshotClasses().Get(className, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("failed to retrieve storage class %s from the API server: %q", className, err)
		//return nil, fmt.Errorf("failed to retrieve storage class %s from the API server: %q", className, err)
	}
	return class, nil
}

// getClaimFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (handler *csiHandler) getClaimFromVolumeSnapshot(snapshot *crdv1.VolumeSnapshot) (*v1.PersistentVolumeClaim, error) {
	pvcName := snapshot.Spec.PersistentVolumeClaimName
	if pvcName == "" {
		return nil, fmt.Errorf("the PVC name is not specified in snapshot %s", snapshotKey(snapshot))
	}

	pvc, err := handler.client.CoreV1().PersistentVolumeClaims(snapshot.Namespace).Get(pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PVC %s from the API server: %q", pvcName, err)
	}
	if pvc.Status.Phase != v1.ClaimBound {
		return nil, fmt.Errorf("the PVC %s not yet bound to a PV, will not attempt to take a snapshot yet", pvcName)
	}

	return pvc, nil
}
