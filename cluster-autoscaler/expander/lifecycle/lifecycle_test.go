package lifecycle

import (
	"testing"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/kubernetes/pkg/scheduler/schedulercache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/api/core/v1"
	"strconv"
	"github.com/stretchr/testify/assert"
)

type FakeNodeGroup struct {
	id string
}

func (f *FakeNodeGroup) MaxSize() int                       { return 2 }
func (f *FakeNodeGroup) MinSize() int                       { return 1 }
func (f *FakeNodeGroup) TargetSize() (int, error)           { return 2, nil }
func (f *FakeNodeGroup) IncreaseSize(delta int) error       { return nil }
func (f *FakeNodeGroup) DecreaseTargetSize(delta int) error { return nil }
func (f *FakeNodeGroup) DeleteNodes([]*v1.Node) error       { return nil }
func (f *FakeNodeGroup) Id() string                         { return f.id }
func (f *FakeNodeGroup) Debug() string                      { return f.id }
func (f *FakeNodeGroup) Nodes() ([]string, error)           { return []string{}, nil }
func (f *FakeNodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}
func (f *FakeNodeGroup) Exist() bool           { return true }
func (f *FakeNodeGroup) Create() error         { return cloudprovider.ErrAlreadyExist }
func (f *FakeNodeGroup) Delete() error         { return cloudprovider.ErrNotImplemented }
func (f *FakeNodeGroup) Autoprovisioned() bool { return false }

func TestLifecycle(t *testing.T) {

	strategy := NewStrategy()

	waitSchedulePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replica-1",
			Labels: map[string]string{
				LabelTier: SearchReplica,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Env: []v1.EnvVar{
						{
							Name:  EnvNameIndexPriority,
							Value: strconv.Itoa(int(oneNormal)),
						},
					},
				},
			},
		},
	}

	scheduledPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replica-1",
			Labels: map[string]string{
				LabelTier: SearchReplica,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Env: []v1.EnvVar{
						{
							Name:  EnvNameIndexPriority,
							Value: strconv.Itoa(int(oneNormal)),
						},
					},
				},
			},
		},
	}

	normalOption := expander.Option{
		NodeGroup: &FakeNodeGroup{
			id: "search-replica-normal-group",
		},
		Pods: []*v1.Pod{
			waitSchedulePod,
		},
	}
	spotOption := expander.Option{
		NodeGroup: &FakeNodeGroup{
			id: "search-replica-spot-group",
		},
		Pods: []*v1.Pod{
			waitSchedulePod,
		},
	}

	normalNode := schedulercache.NewNodeInfo(scheduledPod)
	normalNode.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "normal-node-1",
			Labels: map[string]string{
				NodeLifecycleLabel: lifecycleNormal,
			},
		},
	})
	spotNode := schedulercache.NewNodeInfo()
	spotNode.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spot-node-1",
			Labels: map[string]string{
				NodeLifecycleLabel: lifecycleSpot,
			},
		},
	})
	nodeInfo := make(map[string]*schedulercache.NodeInfo)
	nodeInfo[normalNode.Node().Name] = normalNode
	nodeInfo[spotNode.Node().Name] = spotNode

	option := strategy.BestOption([]expander.Option{normalOption, spotOption}, nodeInfo)
	assert.Equal(t, spotOption.NodeGroup.Id(), option.NodeGroup.Id())

	normalNode1 := schedulercache.NewNodeInfo()
	normalNode1.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "normal-node-1",
			Labels: map[string]string{
				NodeLifecycleLabel: lifecycleNormal,
			},
		},
	})
	spotNode1 := schedulercache.NewNodeInfo(scheduledPod)
	spotNode1.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spot-node-1",
			Labels: map[string]string{
				NodeLifecycleLabel: lifecycleSpot,
			},
		},
	})
	nodeInfo1 := make(map[string]*schedulercache.NodeInfo)
	nodeInfo1[normalNode.Node().Name] = normalNode1
	nodeInfo1[spotNode.Node().Name] = spotNode1
	option = strategy.BestOption([]expander.Option{normalOption, spotOption}, nodeInfo1)
	assert.Equal(t, normalOption.NodeGroup.Id(), option.NodeGroup.Id())

}
