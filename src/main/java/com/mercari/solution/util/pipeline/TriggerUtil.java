package com.mercari.solution.util.pipeline;

import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.transforms.windowing.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TriggerUtil {

    public static class TriggerParameters implements Serializable {

        private TriggerType type;

        // for watermark
        private TriggerParameters earlyFiringTrigger;
        private TriggerParameters lateFiringTrigger;

        // for composite triggers
        private List<TriggerParameters> childrenTriggers;

        // for repeatedly
        private TriggerParameters foreverTrigger;

        // for afterProcessingTime
        private Long pastFirstElementDelay;
        private DateTimeUtil.TimeUnit pastFirstElementDelayUnit;

        // for afterPane
        private Integer elementCountAtLeast;

        // final trigger
        private TriggerParameters finalTrigger;


        public TriggerType getType() {
            return type;
        }

        public void setType(TriggerType type) {
            this.type = type;
        }

        public TriggerParameters getEarlyFiringTrigger() {
            return earlyFiringTrigger;
        }

        public void setEarlyFiringTrigger(TriggerParameters earlyFiringTrigger) {
            this.earlyFiringTrigger = earlyFiringTrigger;
        }

        public TriggerParameters getLateFiringTrigger() {
            return lateFiringTrigger;
        }

        public void setLateFiringTrigger(TriggerParameters lateFiringTrigger) {
            this.lateFiringTrigger = lateFiringTrigger;
        }

        public List<TriggerParameters> getChildrenTriggers() {
            return childrenTriggers;
        }

        public void setChildrenTriggers(List<TriggerParameters> childrenTriggers) {
            this.childrenTriggers = childrenTriggers;
        }

        public TriggerParameters getForeverTrigger() {
            return foreverTrigger;
        }

        public void setForeverTrigger(TriggerParameters foreverTrigger) {
            this.foreverTrigger = foreverTrigger;
        }

        public Long getPastFirstElementDelay() {
            return pastFirstElementDelay;
        }

        public void setPastFirstElementDelay(Long pastFirstElementDelay) {
            this.pastFirstElementDelay = pastFirstElementDelay;
        }

        public DateTimeUtil.TimeUnit getPastFirstElementDelayUnit() {
            return pastFirstElementDelayUnit;
        }

        public void setPastFirstElementDelayUnit(DateTimeUtil.TimeUnit pastFirstElementDelayUnit) {
            this.pastFirstElementDelayUnit = pastFirstElementDelayUnit;
        }

        public Integer getElementCountAtLeast() {
            return elementCountAtLeast;
        }

        public void setElementCountAtLeast(Integer elementCountAtLeast) {
            this.elementCountAtLeast = elementCountAtLeast;
        }

        public TriggerParameters getFinalTrigger() {
            return finalTrigger;
        }

        public void setFinalTrigger(TriggerParameters finalTrigger) {
            this.finalTrigger = finalTrigger;
        }

        public void setDefaults() {
            if(this.type == null) {
                this.type = TriggerType.afterWatermark;
            }
        }

    }

    public enum TriggerType {
        afterWatermark,
        afterProcessingTime,
        afterPane,
        repeatedly,
        afterEach,
        afterFirst,
        afterAll
    }

    public static Trigger createTrigger(final TriggerParameters parameter) {
        final Trigger trigger;
        switch (parameter.getType()) {
            case afterWatermark: {
                if(parameter.getEarlyFiringTrigger() != null && parameter.getLateFiringTrigger() != null) {
                    trigger = AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings((Trigger.OnceTrigger) createTrigger(parameter.getEarlyFiringTrigger()))
                            .withLateFirings((Trigger.OnceTrigger) createTrigger(parameter.getLateFiringTrigger()));
                } else if(parameter.getEarlyFiringTrigger() != null) {
                    trigger = AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings((Trigger.OnceTrigger) createTrigger(parameter.getEarlyFiringTrigger()));
                } else if(parameter.getLateFiringTrigger() != null) {
                    trigger = AfterWatermark.pastEndOfWindow()
                            .withLateFirings((Trigger.OnceTrigger) createTrigger(parameter.getLateFiringTrigger()));
                } else {
                    trigger = AfterWatermark.pastEndOfWindow();
                }
                break;
            }
            case afterProcessingTime: {
                final AfterProcessingTime afterProcessingTime = AfterProcessingTime.pastFirstElementInPane();
                trigger = afterProcessingTime.plusDelayOf(
                        DateTimeUtil.getDuration(parameter.getPastFirstElementDelayUnit(), parameter.getPastFirstElementDelay()));
                break;
            }
            case afterPane: {
                trigger = AfterPane.elementCountAtLeast(parameter.getElementCountAtLeast());
                break;
            }
            case afterFirst:
            case afterEach:
            case afterAll: {
                final List<Trigger> triggers = new ArrayList<>();
                for(final TriggerParameters child : parameter.getChildrenTriggers()) {
                    triggers.add(createTrigger(child));
                }
                switch (parameter.getType()) {
                    case afterFirst: {
                        trigger = AfterFirst.of(triggers);
                        break;
                    }
                    case afterEach: {
                        trigger = AfterEach.inOrder(triggers);
                        break;
                    }
                    case afterAll: {
                        trigger = AfterAll.of(triggers);
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Not supported window trigger: " + parameter.getType());
                    }
                }
                break;
            }
            case repeatedly: {
                trigger = Repeatedly.forever(createTrigger(parameter.getForeverTrigger()));
                break;
            }
            default: {
                throw new IllegalArgumentException("Not supported window trigger: " + parameter.getType());
            }
        }

        if(parameter.getFinalTrigger() != null) {
            return trigger.orFinally((Trigger.OnceTrigger) createTrigger(parameter.getFinalTrigger()));
        }

        return trigger;
    }

}
