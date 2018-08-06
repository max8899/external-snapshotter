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
	"fmt"
	"time"

	"github.com/golang/glog"
	crdv1 "github.com/kubernetes-csi/external-snapshotter/pkg/apis/volumesnapshot/v1alpha1"
	clientset "github.com/kubernetes-csi/external-snapshotter/pkg/client/clientset/versioned"
	storageinformers "github.com/kubernetes-csi/external-snapshotter/pkg/client/informers/externalversions/volumesnapshot/v1alpha1"
	storagelisters "github.com/kubernetes-csi/external-snapshotter/pkg/client/listers/volumesnapshot/v1alpha1"
	"github.com/kubernetes-csi/external-snapshotter/pkg/connection"
	"k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
)

type CSISnapshotController struct {
	clientset       clientset.Interface
	client          kubernetes.Interface
	snapshotterName string
	eventRecorder   record.EventRecorder
	snapshotQueue   workqueue.RateLimitingInterface
	contentQueue    workqueue.RateLimitingInterface

	snapshotLister       storagelisters.VolumeSnapshotLister
	snapshotListerSynced cache.InformerSynced
	contentLister        storagelisters.VolumeSnapshotContentLister
	contentListerSynced  cache.InformerSynced

	snapshotStore cache.Store
	contentStore  cache.Store

	handler Handler
	// Map of scheduled/running operations.
	runningOperations goroutinemap.GoRoutineMap

	createSnapshotContentRetryCount int
	createSnapshotContentInterval   time.Duration
	resyncPeriod                    time.Duration
}

// NewCSISnapshotController returns a new *CSISnapshotController
func NewCSISnapshotController(
	clientset clientset.Interface,
	client kubernetes.Interface,
	snapshotterName string,
	volumeSnapshotInformer storageinformers.VolumeSnapshotInformer,
	volumeSnapshotContentInformer storageinformers.VolumeSnapshotContentInformer,
	createSnapshotContentRetryCount int,
	createSnapshotContentInterval time.Duration,
	conn connection.CSIConnection,
	timeout time.Duration,
	resyncPeriod time.Duration,
	snapshotNamePrefix string,
	snapshotNameUUIDLength int,
) *CSISnapshotController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.Core().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("csi-snapshotter %s", snapshotterName)})

	ctrl := &CSISnapshotController{
		clientset:                       clientset,
		client:                          client,
		snapshotterName:                 snapshotterName,
		eventRecorder:                   eventRecorder,
		handler:                         NewCSIHandler(clientset, client, snapshotterName, eventRecorder, conn, timeout, createSnapshotContentRetryCount, createSnapshotContentInterval, snapshotNamePrefix, snapshotNameUUIDLength),
		runningOperations:               goroutinemap.NewGoRoutineMap(true),
		createSnapshotContentRetryCount: createSnapshotContentRetryCount,
		createSnapshotContentInterval:   createSnapshotContentInterval,
		resyncPeriod:                    resyncPeriod,
		snapshotStore:                   cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		contentStore:                    cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		snapshotQueue:                   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-snapshot"),
		contentQueue:                    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-content"),
	}

	volumeSnapshotInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueSnapshotWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueSnapshotWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueSnapshotWork(obj) },
		},
		ctrl.resyncPeriod,
	)
	ctrl.snapshotLister = volumeSnapshotInformer.Lister()
	ctrl.snapshotListerSynced = volumeSnapshotInformer.Informer().HasSynced

	volumeSnapshotContentInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueContentWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueContentWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueContentWork(obj) },
		},
		ctrl.resyncPeriod,
	)
	ctrl.contentLister = volumeSnapshotContentInformer.Lister()
	ctrl.contentListerSynced = volumeSnapshotContentInformer.Informer().HasSynced

	return ctrl
}

func (ctrl *CSISnapshotController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.snapshotQueue.ShutDown()
	defer ctrl.contentQueue.ShutDown()

	glog.Infof("Starting CSI snapshotter")
	defer glog.Infof("Shutting CSI snapshotter")

	if !cache.WaitForCacheSync(stopCh, ctrl.snapshotListerSynced, ctrl.contentListerSynced) {
		glog.Errorf("Cannot sync caches")
		return
	}

	ctrl.initializeCaches(ctrl.snapshotLister, ctrl.contentLister)

	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.snapshotWorker, 0, stopCh)
		go wait.Until(ctrl.contentWorker, 0, stopCh)
	}

	<-stopCh
}

// enqueueSnapshotWork adds snapshot to given work queue.
func (ctrl *CSISnapshotController) enqueueSnapshotWork(obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if vs, ok := obj.(*crdv1.VolumeSnapshot); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(vs)
		if err != nil {
			glog.Errorf("failed to get key from object: %v, %v", err, vs)
			return
		}
		glog.V(5).Infof("enqueued %q for sync", objName)
		ctrl.snapshotQueue.Add(objName)
	}
}

// enqueueContentWork adds snapshot data to given work queue.
func (ctrl *CSISnapshotController) enqueueContentWork(obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if content, ok := obj.(*crdv1.VolumeSnapshotContent); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(content)
		if err != nil {
			glog.Errorf("failed to get key from object: %v, %v", err, content)
			return
		}
		glog.V(5).Infof("enqueued %q for sync", objName)
		ctrl.contentQueue.Add(objName)
	}
}

// snapshotWorker processes items from snapshotQueue. It must run only once,
// syncSnapshot is not assured to be reentrant.
func (ctrl *CSISnapshotController) snapshotWorker() {
	workFunc := func() bool {
		keyObj, quit := ctrl.snapshotQueue.Get()
		if quit {
			return true
		}
		defer ctrl.snapshotQueue.Done(keyObj)
		key := keyObj.(string)
		glog.V(5).Infof("snapshotWorker[%s]", key)

		namespace, name, err := cache.SplitMetaNamespaceKey(key)
		glog.V(5).Infof("snapshotWorker: snapshot namespace [%s] name [%s]", namespace, name)
		if err != nil {
			glog.V(4).Infof("error getting namespace & name of snapshot %q to get snapshot from informer: %v", key, err)
			return false
		}
		snapshot, err := ctrl.snapshotLister.VolumeSnapshots(namespace).Get(name)
		if err == nil {
			if ctrl.shouldProcessSnapshot(snapshot) {
				// The volume snapshot still exists in informer cache, the event must have
				// been add/update/sync
				glog.V(4).Infof("should process snapshot")
				ctrl.updateSnapshot(snapshot)
			}
			return false
		}
		if err != nil && !errors.IsNotFound(err) {
			glog.V(2).Infof("error getting snapshot %q from informer: %v", key, err)
			return false
		}
		// The snapshot is not in informer cache, the event must have been "delete"
		vsObj, found, err := ctrl.snapshotStore.GetByKey(key)
		if err != nil {
			glog.V(2).Infof("error getting snapshot %q from cache: %v", key, err)
			return false
		}
		if !found {
			// The controller has already processed the delete event and
			// deleted the snapshot from its cache
			glog.V(2).Infof("deletion of vs %q was already processed", key)
			return false
		}
		snapshot, ok := vsObj.(*crdv1.VolumeSnapshot)
		if !ok {
			glog.Errorf("expected vs, got %+v", vsObj)
			return false
		}
		if ctrl.shouldProcessSnapshot(snapshot) {
			ctrl.deleteSnapshot(snapshot)
		}
		return false
	}

	for {
		if quit := workFunc(); quit {
			glog.Infof("snapshot worker queue shutting down")
			return
		}
	}
}

// contentWorker processes items from contentQueue. It must run only once,
// syncContent is not assured to be reentrant.
func (ctrl *CSISnapshotController) contentWorker() {
	workFunc := func() bool {
		keyObj, quit := ctrl.contentQueue.Get()
		if quit {
			return true
		}
		defer ctrl.contentQueue.Done(keyObj)
		key := keyObj.(string)
		glog.V(5).Infof("contentWorker[%s]", key)

		_, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			glog.V(4).Infof("error getting name of snapshotData %q to get snapshotData from informer: %v", key, err)
			return false
		}
		content, err := ctrl.contentLister.Get(name)
		if err == nil {
			// The volume still exists in informer cache, the event must have
			// been add/update/sync
			ctrl.updateContent(content)
			return false
		}
		if !errors.IsNotFound(err) {
			glog.V(2).Infof("error getting content %q from informer: %v", key, err)
			return false
		}

		// The content is not in informer cache, the event must have been
		// "delete"
		contentObj, found, err := ctrl.contentStore.GetByKey(key)
		if err != nil {
			glog.V(2).Infof("error getting content %q from cache: %v", key, err)
			return false
		}
		if !found {
			// The controller has already processed the delete event and
			// deleted the volume from its cache
			glog.V(2).Infof("deletion of content %q was already processed", key)
			return false
		}
		content, ok := contentObj.(*crdv1.VolumeSnapshotContent)
		if !ok {
			glog.Errorf("expected content, got %+v", content)
			return false
		}
		ctrl.deleteContent(content)
		return false
	}

	for {
		if quit := workFunc(); quit {
			glog.Infof("content worker queue shutting down")
			return
		}
	}
}

// shouldProcessSnapshot detect if snapshotter in the VolumeSnapshotClass is the same as the snapshotter
// in external controller.
func (ctrl *CSISnapshotController) shouldProcessSnapshot(snapshot *crdv1.VolumeSnapshot) bool {
	class, err := ctrl.handler.GetClassFromVolumeSnapshot(snapshot)
	if err != nil {
		return false
	}
	glog.V(5).Infof("VolumeSnapshotClass Snapshotter [%s] Snapshot Controller snapshotterName [%s]", class.Snapshotter, ctrl.snapshotterName)
	if class.Snapshotter != ctrl.snapshotterName {
		glog.V(4).Infof("Skipping VolumeSnapshot %s for snapshotter [%s] in VolumeSnapshotClass because it does not match with the snapshotter for controller [%s]", snapshotKey(snapshot), class.Snapshotter, ctrl.snapshotterName)
		return false
	}
	return true
}

// updateSnapshot runs in worker thread and handles "snapshot added",
// "snapshot updated" and "periodic sync" events.
func (ctrl *CSISnapshotController) updateSnapshot(vs *crdv1.VolumeSnapshot) {
	// Store the new vs version in the cache and do not process it if this is
	// an old version.
	glog.V(5).Infof("updateSnapshot %q", snapshotKey(vs))
	newVS, err := ctrl.storeSnapshotUpdate(vs)
	if err != nil {
		glog.Errorf("%v", err)
	}
	if !newVS {
		return
	}
	err = ctrl.syncSnapshot(vs)
	if err != nil {
		if errors.IsConflict(err) {
			// Version conflict error happens quite often and the controller
			// recovers from it easily.
			glog.V(3).Infof("could not sync claim %q: %+v", snapshotKey(vs), err)
		} else {
			glog.Errorf("could not sync volume %q: %+v", snapshotKey(vs), err)
		}
	}
}

// updateContent runs in worker thread and handles "content added",
// "content updated" and "periodic sync" events.
func (ctrl *CSISnapshotController) updateContent(content *crdv1.VolumeSnapshotContent) {
	// Store the new vs version in the cache and do not process it if this is
	// an old version.
	new, err := ctrl.storeContentUpdate(content)
	if err != nil {
		glog.Errorf("%v", err)
	}
	if !new {
		return
	}
	err = ctrl.syncContent(content)
	if err != nil {
		if errors.IsConflict(err) {
			// Version conflict error happens quite often and the controller
			// recovers from it easily.
			glog.V(3).Infof("could not sync content %q: %+v", content.Name, err)
		} else {
			glog.Errorf("could not sync content %q: %+v", content.Name, err)
		}
	}
}

// syncContent deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncContent(content *crdv1.VolumeSnapshotContent) error {
	glog.V(4).Infof("synchronizing VolumeSnapshotContent[%s]", content.Name)

	// VolumeSnapshotContent is not bind to any VolumeSnapshot, this case rare and we just return err
	if content.Spec.VolumeSnapshotRef == nil {
		// content is not bind
		glog.V(4).Infof("synchronizing VolumeSnapshotContent[%s]: VolumeSnapshotContent is not bound to any VolumeSnapshot", content.Name)
		return fmt.Errorf("volumeSnapshotContent %s is not bound to any VolumeSnapshot", content.Name)
	} else {
		glog.V(4).Infof("synchronizing VolumeSnapshotContent[%s]: content is bound to snapshot %s", content.Name, snapshotRefKey(content.Spec.VolumeSnapshotRef))
		// The VolumeSnapshotContent is reserved for a VolumeSNapshot;
		// that VolumeSnapshot has not yet been bound to this VolumeSnapshotCent; the VolumeSnapshot sync will handle it.
		if content.Spec.VolumeSnapshotRef.UID == "" {
			glog.V(4).Infof("synchronizing VolumeSnapshotContent[%s]: VolumeSnapshotContent is pre-bound to VolumeSnapshot", content.Name, snapshotRefKey(content.Spec.VolumeSnapshotRef))
			return nil
		}
		// Get the VolumeSnapshot by _name_
		var vs *crdv1.VolumeSnapshot
		vsName := snapshotRefKey(content.Spec.VolumeSnapshotRef)
		obj, found, err := ctrl.snapshotStore.GetByKey(vsName)
		if err != nil {
			return err
		}
		if !found {
			glog.V(4).Infof("synchronizing VolumeSnapshotContent[%s]: vs %s not found", content.Name, snapshotRefKey(content.Spec.VolumeSnapshotRef))
			// Fall through with vs = nil
		} else {
			var ok bool
			vs, ok = obj.(*crdv1.VolumeSnapshot)
			if !ok {
				return fmt.Errorf("cannot convert object from vs cache to vs %q!?: %#v", content.Name, obj)
			}
			glog.V(4).Infof("synchronizing VolumeSnapshotContent[%s]: vs %s found", content.Name, snapshotRefKey(content.Spec.VolumeSnapshotRef))
		}
		if vs != nil && vs.UID != content.Spec.VolumeSnapshotRef.UID {
			// The vs that the content was pointing to was deleted, and another
			// with the same name created.
			glog.V(4).Infof("synchronizing VolumeSnapshotContent[%s]: content %s has different UID, the old one must have been deleted", content.Name, snapshotRefKey(content.Spec.VolumeSnapshotRef))
			// Treat the volume as bound to a missing claim.
			vs = nil
		}
		if vs == nil {
			ctrl.deleteSnapshotContent(content)
		}
	}
	return nil
}

// syncSnapshot is the main controller method to decide what to do with a snapshot.
// It's invoked by appropriate cache.Controller callbacks when a snapshot is
// created, updated or periodically synced. We do not differentiate between
// these events.
// For easier readability, it is split into syncCompleteSnapshot and syncUncompleteSnapshot
func (ctrl *CSISnapshotController) syncSnapshot(snapshot *crdv1.VolumeSnapshot) error {
	glog.V(4).Infof("synchonizing VolumeSnapshot[%s]: %s", snapshotKey(snapshot), getSnapshotStatusForLogging(snapshot))

	if !snapshot.Status.Bound {
		return ctrl.syncUnboundSnapshot(snapshot)
	} else {
		return ctrl.syncBoundSnapshot(snapshot)
	}
}

// syncCompleteSnapshot checks the snapshot which has been bound to snapshot content succesfully before.
// If there is any problem with the binding (e.g., snapshot points to a non-exist snapshot content), update the snapshot status and emit event.
func (ctrl *CSISnapshotController) syncBoundSnapshot(snapshot *crdv1.VolumeSnapshot) error {
	if snapshot.Spec.SnapshotContentName == "" {
		if _, err := ctrl.updateSnapshotErrorStatusWithEvent(snapshot, v1.EventTypeWarning, "SnapshotLost", "Bound snapshot has lost reference to VolumeSnapshotContent"); err != nil {
			return err
		}
		return nil
	}
	obj, found, err := ctrl.contentStore.GetByKey(snapshot.Spec.SnapshotContentName)
	if err != nil {
		return err
	}
	if !found {
		if _, err = ctrl.updateSnapshotErrorStatusWithEvent(snapshot, v1.EventTypeWarning, "SnapshotLost", "Bound snapshot has lost reference to VolumeSnapshotContent"); err != nil {
			return err
		}
		return nil
	} else {
		content, ok := obj.(*crdv1.VolumeSnapshotContent)
		if !ok {
			return fmt.Errorf("Cannot convert object from snapshot content store to VolumeSnapshotContent %q!?: %#v", snapshot.Spec.SnapshotContentName, obj)
		}

		glog.V(4).Infof("syncCompleteSnapshot[%s]: VolumeSnapshotContent %q found", snapshotKey(snapshot), content.Name)
		if !IsSnapshotBound(snapshot, content) {
			// snapshot is bound but content is not bound to snapshot correctly
			if _, err = ctrl.updateSnapshotErrorStatusWithEvent(snapshot, v1.EventTypeWarning, "SnapshotMisbound", "VolumeSnapshotContent is not bound to the VolumeSnapshot correctly"); err != nil {
				return err
			}
			return nil
		}
		// Snapshot is correctly bound.
		if _, err = ctrl.updateSnapshotBoundWithEvent(snapshot, v1.EventTypeNormal, "SnapshotBound", "Snapshot is bound to its VolumeSnapshotContent"); err != nil {
			return err
		}
		return nil
	}
}

func (ctrl *CSISnapshotController) syncUnboundSnapshot(snapshot *crdv1.VolumeSnapshot) error {
	uniqueSnapshotName := snapshotKey(snapshot)
	glog.V(4).Infof("syncSnapshot %s", uniqueSnapshotName)

	// Snsapshot has errors during its creation. Controller will not try to fix it. Nothing to do.
	if snapshot.Status.Error != nil {
		return nil
	}

	if snapshot.Spec.SnapshotContentName != "" {
		contentObj, found, err := ctrl.contentStore.GetByKey(snapshot.Spec.SnapshotContentName)
		if err != nil {
			return err
		}
		if !found {
			// snapshot is bound to a non-existing content.
			return fmt.Errorf("snapshot %s is bound to a non-existing content %s", uniqueSnapshotName, snapshot.Spec.SnapshotContentName)
		}
		content, ok := contentObj.(*crdv1.VolumeSnapshotContent)
		if !ok {
			return fmt.Errorf("expected volume snapshot content, got %+v", contentObj)
		}
		
		if err := ctrl.BindSnapshotContent(snapshot, content); err != nil {
			// snapshot is bound but content is not bound to snapshot correctly
			return fmt.Errorf("snapshot %s is bound, but VolumeSnapshotContent %s is not bound to the VolumeSnapshot correctly, %v", uniqueSnapshotName, content.Name, err)
		}

		// snapshot is already bound correctly, check the status and update if it is ready.
		glog.V(4).Infof("Check and update snapshot %s status", uniqueSnapshotName)
		if err = ctrl.checkandUpdateSnapshotStatus(snapshot, content); err != nil {
			return err
		}
		return nil
	} else { // snapshot.Spec.SnapshotContentName == nil
		if ContentObj := ctrl.getMatchSnapshotContent(snapshot); ContentObj != nil {
			glog.V(4).Infof("Find VolumeSnapshotContent object %s for snapshot %s", ContentObj.Name, uniqueSnapshotName)
			result, err := ctrl.handler.BindandUpdateVolumeSnapshot(ContentObj, snapshot)
			if err != nil {
				return err
			}
			_, err = ctrl.storeSnapshotUpdate(result)
			if err != nil {
				// We will get an "snapshot update" event soon, this is not a big error
				glog.Warning("syncSnapshot [%s]: cannot update internal cache: %v", snapshotKey(snapshot), err)
			}
			return nil
		} else {
			if err := ctrl.createSnapshot(snapshot); err != nil {
				return err
			}
			return nil
		}

	}
}

// getMatchSnapshotContent looks up VolumeSnapshotContent for a VolumeSnapshot named snapshotName
func (ctrl *CSISnapshotController) getMatchSnapshotContent(vs *crdv1.VolumeSnapshot) *crdv1.VolumeSnapshotContent {
	var snapshotDataObj *crdv1.VolumeSnapshotContent
	var found bool

	objs := ctrl.contentStore.List()
	for _, obj := range objs {
		content := obj.(*crdv1.VolumeSnapshotContent)
		if content.Spec.VolumeSnapshotRef != nil &&
			content.Spec.VolumeSnapshotRef.Name == vs.Name &&
			content.Spec.VolumeSnapshotRef.Namespace == vs.Namespace &&
			content.Spec.VolumeSnapshotRef.UID == vs.UID {
			found = true
			snapshotDataObj = content
			break
		}
	}

	if !found {
		glog.V(4).Infof("No VolumeSnapshotContent for VolumeSnapshot %s found", snapshotKey(vs))
		return nil
	}

	return snapshotDataObj
}

// deleteSnapshot runs in worker thread and handles "snapshot deleted" event.
func (ctrl *CSISnapshotController) deleteSnapshot(vs *crdv1.VolumeSnapshot) {
	_ = ctrl.snapshotStore.Delete(vs)
	glog.V(4).Infof("vs %q deleted", snapshotKey(vs))

	snapshotContentName := vs.Spec.SnapshotContentName
	if snapshotContentName == "" {
		glog.V(5).Infof("deleteSnapshot[%q]: content not bound", snapshotKey(vs))
		return
	}
	// sync the content when its vs is deleted.  Explicitly sync'ing the
	// content here in response to vs deletion prevents the content from
	// waiting until the next sync period for its Release.
	glog.V(5).Infof("deleteSnapshot[%q]: scheduling sync of content %s", snapshotKey(vs), snapshotContentName)
	ctrl.contentQueue.Add(snapshotContentName)
}

// deleteContent runs in worker thread and handles "snapshot deleted" event.
func (ctrl *CSISnapshotController) deleteContent(content *crdv1.VolumeSnapshotContent) {
	_ = ctrl.contentStore.Delete(content)
	glog.V(4).Infof("content %q deleted", content.Name)

	snapshotName := snapshotRefKey(content.Spec.VolumeSnapshotRef)
	if snapshotName == "" {
		glog.V(5).Infof("deleteContent[%q]: content not bound", content.Name)
		return
	}
	// sync the vs when its vs is deleted.  Explicitly sync'ing the
	// vs here in response to content deletion prevents the vs from
	// waiting until the next sync period for its Release.
	glog.V(5).Infof("deleteContent[%q]: scheduling sync of vs %s", content.Name, snapshotName)
	ctrl.contentQueue.Add(snapshotName)
}

// initializeCaches fills all controller caches with initial data from etcd in
// order to have the caches already filled when first addSnapshot/addContent to
// perform initial synchronization of the controller.
func (ctrl *CSISnapshotController) initializeCaches(snapshotLister storagelisters.VolumeSnapshotLister, contentLister storagelisters.VolumeSnapshotContentLister) {
	vsList, err := snapshotLister.List(labels.Everything())
	if err != nil {
		glog.Errorf("CSISnapshotController can't initialize caches: %v", err)
		return
	}
	for _, vs := range vsList {
		vsClone := vs.DeepCopy()
		if _, err = ctrl.storeSnapshotUpdate(vsClone); err != nil {
			glog.Errorf("error updating volume snapshot cache: %v", err)
		}
	}

	contentList, err := contentLister.List(labels.Everything())
	if err != nil {
		glog.Errorf("CSISnapshotController can't initialize caches: %v", err)
		return
	}
	for _, content := range contentList {
		contentClone := content.DeepCopy()
		if _, err = ctrl.storeSnapshotUpdate(contentClone); err != nil {
			glog.Errorf("error updating volume snapshot cache: %v", err)
		}
	}

	glog.V(4).Infof("controller initialized")
}

// deleteSnapshotContent starts delete action.
func (ctrl *CSISnapshotController) deleteSnapshotContent(content *crdv1.VolumeSnapshotContent) {
	operationName := fmt.Sprintf("delete-%s[%s]", content.Name, string(content.UID))
	glog.V(4).Infof("Snapshotter is about to delete volume snapshot and the operation named %s", operationName)
	ctrl.scheduleOperation(operationName, func() error {
		return ctrl.handler.DeleteSnapshotContentOperation(content)
	})
}

// scheduleOperation starts given asynchronous operation on given volume. It
// makes sure the operation is already not running.
func (ctrl *CSISnapshotController) scheduleOperation(operationName string, operation func() error) {
	glog.V(4).Infof("scheduleOperation[%s]", operationName)

	err := ctrl.runningOperations.Run(operationName, operation)
	if err != nil {
		switch {
		case goroutinemap.IsAlreadyExists(err):
			glog.V(4).Infof("operation %q is already running, skipping", operationName)
		case exponentialbackoff.IsExponentialBackoff(err):
			glog.V(4).Infof("operation %q postponed due to exponential backoff", operationName)
		default:
			glog.Errorf("error scheduling operation %q: %v", operationName, err)
		}
	}
}

func (ctrl *CSISnapshotController) storeSnapshotUpdate(vs interface{}) (bool, error) {
	return storeObjectUpdate(ctrl.snapshotStore, vs, "vs")
}

func (ctrl *CSISnapshotController) storeContentUpdate(content interface{}) (bool, error) {
	return storeObjectUpdate(ctrl.contentStore, content, "content")
}

// createSnapshot starts new asynchronous operation to create snapshot data for snapshot
func (ctrl *CSISnapshotController) createSnapshot(vs *crdv1.VolumeSnapshot) error {
	glog.V(4).Infof("createSnapshot[%s]: started", snapshotKey(vs))
	opName := fmt.Sprintf("create-%s[%s]", snapshotKey(vs), string(vs.UID))
	ctrl.scheduleOperation(opName, func() error {
		snapshotObj, err := ctrl.handler.CreateSnapshotOperation(vs)
		if err != nil {
			glog.Errorf("createSnapshot [%s]: error occurred in createSnapshotOperation: %v", opName, err)
			return err
		}
		_, updateErr := ctrl.storeSnapshotUpdate(snapshotObj)
		if updateErr != nil {
			// We will get an "snapshot update" event soon, this is not a big error
			glog.V(4).Infof("createSnapshot [%s]: cannot update internal cache: %v", snapshotKey(snapshotObj), updateErr)
		}

		return nil
	})
	return nil
}

func (ctrl *CSISnapshotController) checkandUpdateSnapshotStatus(snapshot *crdv1.VolumeSnapshot, content *crdv1.VolumeSnapshotContent) error {
	glog.V(5).Infof("checkandUpdateSnapshotStatus[%s] started", snapshotKey(snapshot))
	opName := fmt.Sprintf("check-%s[%s]", snapshotKey(snapshot), string(snapshot.UID))
	ctrl.scheduleOperation(opName, func() error {
		snapshotObj, err := ctrl.handler.CheckandUpdateSnapshotStatusOperation(snapshot, content)
		if err != nil {
			glog.Errorf("checkandUpdateSnapshotStatus [%s]: error occured %v", snapshotKey(snapshot), err)
			return err
		}
		_, updateErr := ctrl.storeSnapshotUpdate(snapshotObj)
		if updateErr != nil {
			// We will get an "snapshot update" event soon, this is not a big error
			glog.V(4).Infof("checkandUpdateSnapshotStatus [%s]: cannot update internal cache: %v", snapshotKey(snapshotObj), updateErr)
		}

		return nil
	})
	return nil
}

// updateSnapshotStatusWithEvent saves new snapshot.Status to API server and emits
// given event on the snapshot. It saves the status and emits the event only when
// the status has actually changed from the version saved in API server.
// Parameters:
//   snapshot - snapshot to update
//   eventtype, reason, message - event to send, see EventRecorder.Event()
func (ctrl *CSISnapshotController) updateSnapshotErrorStatusWithEvent(snapshot *crdv1.VolumeSnapshot, eventtype, reason, message string) (*crdv1.VolumeSnapshot, error) {
	glog.V(4).Infof("updateSnapshotStatusWithEvent[%s]", snapshotKey(snapshot))

	// Emit the event only when the status change happens
	ctrl.eventRecorder.Event(snapshot, eventtype, reason, message)

	if snapshot.Status.Error != nil && snapshot.Status.Bound == false {
		glog.V(4).Infof("updateClaimStatusWithEvent[%s]: error %v already set", snapshot.Status.Error)
		return snapshot, nil
	}
	snapshotClone := snapshot.DeepCopy()
	if snapshot.Status.Error == nil {
		statusError := &storage.VolumeError{
			Time: metav1.Time{
				Time: time.Now(),
			},
			Message: message,
		}	
		snapshotClone.Status.Error = statusError
	}
	snapshotClone.Status.Bound = false
	newSnapshot, err := ctrl.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshotClone.Namespace).Update(snapshotClone)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshot[%s] error status failed %v", snapshotKey(snapshot), err)
		return newSnapshot, err
	}

	_, err = ctrl.storeSnapshotUpdate(newSnapshot)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshot[%s] error status: cannot update internal cache %v", snapshotKey(snapshot), err)
		return newSnapshot, err
	}
	// Emit the event only when the status change happens
	ctrl.eventRecorder.Event(snapshot, eventtype, reason, message)

	return newSnapshot, nil
}

func (ctrl *CSISnapshotController) updateSnapshotBoundWithEvent(snapshot *crdv1.VolumeSnapshot, eventtype, reason, message string) (*crdv1.VolumeSnapshot, error) {
	glog.V(4).Infof("updateSnapshotBoundWithEvent[%s]", snapshotKey(snapshot))
	if snapshot.Status.Bound && snapshot.Status.Error == nil {
		// Nothing to do.
		glog.V(4).Infof("updateSnapshotBoundWithEvent[%s]: Bound %v already set", snapshot.Status.Bound)
		return snapshot, nil
	}

	// Emit the event only when the status change happens
	//ctrl.eventRecorder.Event(snapshot, eventtype, reason, message)

	snapshotClone := snapshot.DeepCopy()
	snapshotClone.Status.Bound = true
	snapshotClone.Status.Error = nil
	newSnapshot, err := ctrl.clientset.VolumesnapshotV1alpha1().VolumeSnapshots(snapshotClone.Namespace).Update(snapshotClone)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshot[%s] error status failed %v", snapshotKey(snapshot), err)
		return newSnapshot, err
	}
	// Emit the event only when the status change happens
	ctrl.eventRecorder.Event(snapshot, eventtype, reason, message)

	_, err = ctrl.storeSnapshotUpdate(newSnapshot)
	if err != nil {
		glog.V(4).Infof("updating VolumeSnapshot[%s] error status: cannot update internal cache %v", snapshotKey(snapshot), err)
		return newSnapshot, err
	}


	return newSnapshot, nil
}


// Stateless functions
func getSnapshotStatusForLogging(snapshot *crdv1.VolumeSnapshot) string {
	return fmt.Sprintf("bound to: %q, Completed: %v", snapshot.Spec.SnapshotContentName, snapshot.Status.Bound)
}


func IsSnapshotBound(snapshot *crdv1.VolumeSnapshot, content *crdv1.VolumeSnapshotContent) bool {
	if content.Spec.VolumeSnapshotRef != nil && content.Spec.VolumeSnapshotRef.Name == snapshot.Name &&
		content.Spec.VolumeSnapshotRef.UID == snapshot.UID {
		return true
	} 
	return false
}


func (ctrl *CSISnapshotController) BindSnapshotContent(snapshot *crdv1.VolumeSnapshot, content *crdv1.VolumeSnapshotContent) error {
	if content.Spec.VolumeSnapshotRef == nil || content.Spec.VolumeSnapshotRef.Name != snapshot.Name {
		return fmt.Errorf("Could not bind snapshot %s and content %s, the VolumeSnapshotRef does not match", snapshot.Name, content.Name)
	} else if content.Spec.VolumeSnapshotRef.UID != "" && content.Spec.VolumeSnapshotRef.UID != snapshot.UID {
		return fmt.Errorf("Could not bind snapshot %s and content %s, the VolumeSnapshotRef does not match", snapshot.Name, content.Name)
	} else if  content.Spec.VolumeSnapshotRef.UID == "" {
		content.Spec.VolumeSnapshotRef.UID = snapshot.UID
		newContent, err := ctrl.clientset.VolumesnapshotV1alpha1().VolumeSnapshotContents().Update(content)
		if err != nil {
			glog.V(4).Infof("updating VolumeSnapshotContent[%s] error status failed %v", content.Name, err)
			return err
		}
		_, err = ctrl.storeContentUpdate(newContent)
		if err != nil {
			glog.V(4).Infof("updating VolumeSnapshotContent[%s] error status: cannot update internal cache %v", newContent.Name, err)
			return err
		}
	}
	return nil
}