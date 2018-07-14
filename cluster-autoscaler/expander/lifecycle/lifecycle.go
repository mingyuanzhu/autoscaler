package lifecycle

import (
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/expander/random"
	"k8s.io/kubernetes/pkg/scheduler/schedulercache"
	"k8s.io/api/core/v1"
	"strings"
	"github.com/golang/glog"
	"strconv"
)

const (
	lifecycleNormal      = "normal"
	lifecycleSpot        = "spot"
	DefaultLifecycle     = "spot"
	LabelTier            = "tier"
	SearchReplica        = "search-replica"
	EnvNameIndexPriority = "indexPriority"
	NodeLifecycleLabel   = "visenze.node.aws.lifecycle"
)

type SchedulePriority int

const (
	ignore    SchedulePriority = iota // 0
	oneNormal                         // 1
	allNormal                         // 2
	allSpot                           // 3
	allRandom                         // 4
)

// lifecycle when config two node group to auto scale,
// if the scheduler extender want to launch the pod to some special lifecycle(normal or spot) can return this type group
type lifecycle struct {
	fallbackStrategy     expander.Strategy
	lifecycleLabelName   string
	indexPriorityEnvName string
}

// NewStrategy returns a strategy that selects the best scale up option based on which node group returns the lifecycle
func NewStrategy() expander.Strategy {
	return &lifecycle{
		fallbackStrategy:     random.NewStrategy(),
		lifecycleLabelName:   NodeLifecycleLabel,
		indexPriorityEnvName: EnvNameIndexPriority,
	}
}

// BestOption
func (l *lifecycle) BestOption(expansionOptions []expander.Option, nodeInfo map[string]*schedulercache.NodeInfo) *expander.Option {
	var replicaPod *v1.Pod
	for _, option := range expansionOptions {
		for _, pod := range option.Pods {
			if pod.Labels[LabelTier] == SearchReplica {
				replicaPod = pod
				break
			}
		}
	}
	if replicaPod == nil {
		glog.V(1).Info("no replica pod")
		return l.fallbackStrategy.BestOption(expansionOptions, nodeInfo)
	}
	lifecycles := l.scheduleNodeLifecycle(replicaPod, nodeInfo)
	glog.V(1).Infof("pod=%s should scale lifecycle=%s", replicaPod.Name, lifecycles)
	newOptions := make([]expander.Option, 0)
	for _, option := range expansionOptions {
		for _, lifecycle := range lifecycles {
			if strings.Contains(option.NodeGroup.Id(), lifecycle) {
				newOptions = append(newOptions, option)
				break
			}
		}
	}
	if len(newOptions) == 0 {
		glog.V(1).Infof("no group can match lifecycle=%s", lifecycles)
		return nil
	} else if len(newOptions) == 1 {
		return &newOptions[0]
	}
	return l.fallbackStrategy.BestOption(newOptions, nodeInfo)

}

func (l *lifecycle) scheduleNodeLifecycle(p *v1.Pod, nodeInfo map[string]*schedulercache.NodeInfo) []string {
	var (
		isScheduledNormal bool
		isScheduledSpot   bool
	)
	schedulePriority := l.getSchedulePriority(p)
	glog.V(1).Infof("pod already allocate in normal=%t spot=%t scheduler_priority=%d", isScheduledNormal, isScheduledSpot, schedulePriority)
	switch schedulePriority {
	case allNormal:
		return []string{lifecycleNormal}
	case allSpot:
		return []string{lifecycleSpot}
	case oneNormal:
		sameLabelPods := make(map[string]*v1.Pod)
		for _, node := range nodeInfo {
			for _, pod := range node.Pods() {
				if isWithSameLabel(p, pod) {
					sameLabelPods[pod.Name] = pod
				}
				lifecycle := node.Node().Labels[l.lifecycleLabelName]
				if lifecycle == lifecycleNormal {
					isScheduledNormal = true
				} else if lifecycle == lifecycleSpot {
					isScheduledSpot = true
				} else {
					glog.Errorf("node=%s lifecycle label value=%s error", node, lifecycle)
				}
			}
		}
		if isScheduledNormal {
			return []string{lifecycleSpot}
		} else {
			return []string{lifecycleNormal}
		}
	}

	return []string{lifecycleNormal, lifecycleSpot}
}

func (l *lifecycle) getSchedulePriority(pod *v1.Pod) (SchedulePriority) {
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == l.indexPriorityEnvName {
			if i, err := strconv.Atoi(env.Value); err != nil {
				return ignore
			} else {
				return SchedulePriority(i)
			}
		}
	}
	glog.Errorf("can not find scheduler priority by env %s", l.indexPriorityEnvName)
	return ignore
}

// isWithSameLabel whether the two pods has the same label
func isWithSameLabel(pod *v1.Pod, other *v1.Pod) bool {
	if len(pod.Labels) != len(pod.Labels) {
		return false
	}
	for key, value := range pod.Labels {
		if v, ok := other.Labels[key]; ok {
			if v != value {
				return false
			}
		} else {
			return false
		}
	}
	return true
}
