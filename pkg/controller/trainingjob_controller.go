package controller

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	extv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	extclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	paddlev1 "github.com/paddlepaddle/edl/pkg/apis/paddlepaddle/v1"
	paddleclient "github.com/paddlepaddle/edl/pkg/client/clientset/versioned"
	paddlescheme "github.com/paddlepaddle/edl/pkg/client/clientset/versioned/scheme"
	paddleinformers "github.com/paddlepaddle/edl/pkg/client/informers/externalversions"
	paddlelisters "github.com/paddlepaddle/edl/pkg/client/listers/paddlepaddle/v1"
	"github.com/paddlepaddle/edl/pkg/updater"
	"github.com/paddlepaddle/edl/pkg/autoscaler"
)

type TrainingJobController struct {
	// KubeCli is a standard kubernetes clientset
	KubeCli kubernetes.Interface
	// ExtCli is the extension kubernetes clientset
	ExtCli extclient.Interface
	// PaddleCli is a clientset for our own API group
	PaddleCli paddleclient.Interface

	trainingjobLister paddlelisters.TrainingJobLister
	trainingjobSynced cache.InformerSynced

	// jobupdater is
	jobupdater *sync.Map

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// New returns a trainingjob controller
func New(
	kubeCli kubernetes.Interface,
	extCli extclient.Interface,
	paddleCli paddleclient.Interface,
	paddleInformers paddleinformers.SharedInformerFactory) *TrainingJobController {
	trainingjobInformer := paddleInformers.Paddlepaddle().V1().TrainingJobs()
	paddlescheme.AddToScheme(scheme.Scheme)

	glog.V(4).Info("Creating trainingjob event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeCli.CoreV1().Events("")})
	workqueue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "TrainingJob")
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "TrainingJobController"})

	controller := &TrainingJobController{
		KubeCli:           kubeCli,
		ExtCli:            extCli,
		PaddleCli:         paddleCli,
		trainingjobLister: trainingjobInformer.Lister(),
		trainingjobSynced: trainingjobInformer.Informer().HasSynced,
		jobupdater:        new(sync.Map),
		workqueue:         workqueue,
		recorder:          recorder,
	}

	glog.Info("Setting up event handlers")
	trainingjobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueue,
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldTj := oldObj.(*paddlev1.TrainingJob)
			newTj := newObj.(*paddlev1.TrainingJob)
			if oldTj.ResourceVersion == newTj.ResourceVersion {
				glog.V(4).Infof("same resourceversion for training job %s/%s, skipped", oldTj.Namespace, oldTj.Name)
				return
			}
			glog.V(4).Infof("resourceversion for training job %s/%s updated", oldTj.Namespace, oldTj.Name)
			controller.enqueue(newObj)
		},
		DeleteFunc: controller.dequeue,
	})

	return controller
}

// Run will set up the event handlers for trainingjob, as well as syncing
// informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *TrainingJobController) Run(threadiness int, maxLoadDesired float64, stopCh <-chan struct{}) error {
	// TODO add a lock to ensure there is only one controller in the cluster
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	glog.Info("Starting trainingjob controller")
	glog.Info("Starting to create custom resource definition")

	if err := c.createCRD(); err != nil {
		return fmt.Errorf("failed to create kind TrainingJob: %v", err)
	}

	glog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.trainingjobSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	glog.Info("Started workers")
	glog.Info("Started autoscaler")
	as := autoscaler.NewAutoscaler(c.KubeCli, c.jobupdater, autoscaler.WithMaxLoadDesired(maxLoadDesired))
	as.Run()


	<-stopCh
	glog.Info("Shutting down workers")

	return nil
}

func (c *TrainingJobController) createCRD() error {
	crd := &extv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: paddlev1.CRDName(),
		},
		Spec: extv1beta1.CustomResourceDefinitionSpec{
			Group:   paddlev1.CRDGroup,
			Version: paddlev1.CRDVersion,
			Scope:   extv1beta1.NamespaceScoped,
			Names: extv1beta1.CustomResourceDefinitionNames{
				Kind:       paddlev1.CRDKind,
				Plural:     paddlev1.CRDKindPlural,
				ShortNames: []string{paddlev1.CRDShortName},
			},
		},
	}

	_, err := c.ExtCli.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		glog.Errorf("Failed to create crd, error: %s", err.Error())
		return err
	}

	return nil
}

// enqueue takes a TrainingJob resource and converts it into a namespace/name
// string which is then put onto the work queue.
func (c *TrainingJobController) enqueue(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	glog.Infof("enqueue key: %v", key)
	c.workqueue.AddRateLimited(key)
}

func (c *TrainingJobController) dequeue(obj interface{}) {
	job, ok := obj.(*paddlev1.TrainingJob)
	key := job.Namespace + "/" + job.Name
	glog.Infof("dequeue key: %v", key)
	jobToDelete, ok := c.jobupdater.Load(key)
	if !ok {
		glog.Warningf("unsafe state. %s was never created but we received delete event", key)
		return
	}

	oldtj := jobToDelete.(*updater.TrainingJobUpdater)
	oldtj.Delete()
	c.jobupdater.Delete(key)
}

func (c *TrainingJobController) runWorker() {
	for c.processNestWorkItem() {
	}
}

func (c *TrainingJobController) processNestWorkItem() bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}

		c.workqueue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

func (c *TrainingJobController) syncHandler(key string) error {
	glog.Infof("syncHandler, key: %s", key)
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	glog.Infof("syncHandler, ns: %s, name: %s", ns, name)

	job, createErr := c.trainingjobLister.TrainingJobs(ns).Get(name)
	if createErr != nil {
		glog.Errorf("get trainingjob error: %v", err)
		if apierrors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("trainingjob '%s' in the work queue no longer exists", key))
			return nil
		}

		return err
	}

	_, ok := c.jobupdater.Load(key)
	if !ok {
		glog.Infof("create new job tracker, key: '%s'", key)
		nj, _ := updater.NewUpdater(job, c.KubeCli, c.PaddleCli)
		c.jobupdater.Store(key, nj)
	}

	return nil
}
