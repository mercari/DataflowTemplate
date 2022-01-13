package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WindowTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(WindowTransform.class);

    private class WindowTransformParameters {

        private Type type;
        private Unit unit;
        private Long size;
        private Long frequency;
        private Long gap;
        private Long offset;
        private Long allowedLateness;
        private String timezone;

        private Integer startingYear;
        private Integer startingMonth;
        private Integer startingDay;

        private TriggerParameter trigger;

        private Boolean discardingFiredPanes;

        private TimestampCombiner timestampCombiner;

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public Unit getUnit() {
            return unit;
        }

        public void setUnit(Unit unit) {
            this.unit = unit;
        }

        public Long getSize() {
            return size;
        }

        public void setSize(Long size) {
            this.size = size;
        }

        public Long getFrequency() {
            return frequency;
        }

        public void setFrequency(Long frequency) {
            this.frequency = frequency;
        }

        public Long getGap() {
            return gap;
        }

        public void setGap(Long gap) {
            this.gap = gap;
        }

        public Long getOffset() {
            return offset;
        }

        public void setOffset(Long offset) {
            this.offset = offset;
        }

        public Long getAllowedLateness() {
            return allowedLateness;
        }

        public void setAllowedLateness(Long allowedLateness) {
            this.allowedLateness = allowedLateness;
        }

        public String getTimezone() {
            return timezone;
        }

        public void setTimezone(String timezone) {
            this.timezone = timezone;
        }

        public Integer getStartingYear() {
            return startingYear;
        }

        public void setStartingYear(Integer startingYear) {
            this.startingYear = startingYear;
        }

        public Integer getStartingMonth() {
            return startingMonth;
        }

        public void setStartingMonth(Integer startingMonth) {
            this.startingMonth = startingMonth;
        }

        public Integer getStartingDay() {
            return startingDay;
        }

        public void setStartingDay(Integer startingDay) {
            this.startingDay = startingDay;
        }

        public TriggerParameter getTrigger() {
            return trigger;
        }

        public void setTrigger(TriggerParameter trigger) {
            this.trigger = trigger;
        }

        public Boolean getDiscardingFiredPanes() {
            return discardingFiredPanes;
        }

        public void setDiscardingFiredPanes(Boolean discardingFiredPanes) {
            this.discardingFiredPanes = discardingFiredPanes;
        }

        public TimestampCombiner getTimestampCombiner() {
            return timestampCombiner;
        }

        public void setTimestampCombiner(TimestampCombiner timestampCombiner) {
            this.timestampCombiner = timestampCombiner;
        }

    }

    private class TriggerParameter implements Serializable {

        private TriggerType type;

        // for watermark
        private TriggerParameter earlyFiringTrigger;
        private TriggerParameter lateFiringTrigger;

        // for composite triggers
        private List<TriggerParameter> childrenTriggers;

        // for repeatedly
        private TriggerParameter foreverTrigger;

        // for afterProcessingTime
        private Long pastFirstElementDelay;
        private Unit pastFirstElementDelayUnit;

        // for afterPane
        private Integer elementCountAtLeast;

        // final trigger
        private TriggerParameter finalTrigger;

        public TriggerType getType() {
            return type;
        }

        public void setType(TriggerType type) {
            this.type = type;
        }

        public TriggerParameter getEarlyFiringTrigger() {
            return earlyFiringTrigger;
        }

        public void setEarlyFiringTrigger(TriggerParameter earlyFiringTrigger) {
            this.earlyFiringTrigger = earlyFiringTrigger;
        }

        public TriggerParameter getLateFiringTrigger() {
            return lateFiringTrigger;
        }

        public void setLateFiringTrigger(TriggerParameter lateFiringTrigger) {
            this.lateFiringTrigger = lateFiringTrigger;
        }

        public List<TriggerParameter> getChildrenTriggers() {
            return childrenTriggers;
        }

        public void setChildrenTriggers(List<TriggerParameter> childrenTriggers) {
            this.childrenTriggers = childrenTriggers;
        }

        public TriggerParameter getForeverTrigger() {
            return foreverTrigger;
        }

        public void setForeverTrigger(TriggerParameter foreverTrigger) {
            this.foreverTrigger = foreverTrigger;
        }

        public TriggerParameter getFinalTrigger() {
            return finalTrigger;
        }

        public void setFinalTrigger(TriggerParameter finalTrigger) {
            this.finalTrigger = finalTrigger;
        }

        public Long getPastFirstElementDelay() {
            return pastFirstElementDelay;
        }

        public void setPastFirstElementDelay(Long pastFirstElementDelay) {
            this.pastFirstElementDelay = pastFirstElementDelay;
        }

        public Unit getPastFirstElementDelayUnit() {
            return pastFirstElementDelayUnit;
        }

        public void setPastFirstElementDelayUnit(Unit pastFirstElementDelayUnit) {
            this.pastFirstElementDelayUnit = pastFirstElementDelayUnit;
        }

        public Integer getElementCountAtLeast() {
            return elementCountAtLeast;
        }

        public void setElementCountAtLeast(Integer elementCountAtLeast) {
            this.elementCountAtLeast = elementCountAtLeast;
        }
    }

    public enum Type {
        global,
        fixed,
        sliding,
        session,
        calendar
    }

    public enum Unit {
        second,
        minute,
        hour,
        day,
        week,
        month,
        year
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

    public String getName() { return "window"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return WindowTransform.transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final WithWindow transform = new WithWindow(config);
        final Map<String, FCollection<?>> collections = new HashMap<>();
        for(final FCollection input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final Coder coder = input.getCollection().getCoder();
            final PCollection<?> output = ((PCollection<?>) (input.getCollection()).apply(config.getName(), transform))
                    .setCoder(coder);
            collections.put(name, FCollection.update(input, name, output));
        }
        return collections;
    }

    public static class WithWindow<T> extends PTransform<PCollection<T>, PCollection<T>> {

        private final WindowTransformParameters parameters;

        public WindowTransformParameters getParameters() {
            return parameters;
        }

        private WithWindow(final TransformConfig config) {
            this.parameters = new Gson().fromJson(config.getParameters(), WindowTransformParameters.class);
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {
            validate();
            setDefaultParameters();

            Window<T> window;
            switch (parameters.getType()) {
                case global: {
                    window = Window.into(new GlobalWindows());
                    break;
                }
                case fixed: {
                    window = Window.into(FixedWindows
                            .of(getDuration(parameters.getUnit(), parameters.getSize()))
                            .withOffset(getDuration(parameters.getUnit(), parameters.getOffset())));
                    break;
                }
                case sliding: {
                    window = Window.into(SlidingWindows
                            .of(getDuration(parameters.getUnit(), parameters.getSize()))
                            .every(getDuration(parameters.getUnit(), parameters.getFrequency()))
                            .withOffset(getDuration(parameters.getUnit(), parameters.getOffset())));
                    break;
                }
                case session: {
                    window = Window.into(Sessions
                            .withGapDuration(getDuration(parameters.getUnit(), parameters.getGap())));
                    break;
                }
                case calendar: {
                    switch (parameters.getUnit()) {
                        case day: {
                            window = Window.into(CalendarWindows
                                    .days(parameters.getSize().intValue())
                                    .withTimeZone(DateTimeZone.forID(parameters.getTimezone()))
                                    .withStartingDay(
                                            parameters.getStartingYear(),
                                            parameters.getStartingMonth(),
                                            parameters.getStartingDay()));
                            break;
                        }
                        case month: {
                            window = Window.into(CalendarWindows
                                    .months(parameters.getSize().intValue())
                                    .withTimeZone(DateTimeZone.forID(parameters.getTimezone()))
                                    .withStartingMonth(
                                            parameters.getStartingYear(),
                                            parameters.getStartingMonth()));
                            break;
                        }
                        case year: {
                            window = Window.into(CalendarWindows
                                    .years(parameters.getSize().intValue())
                                    .withTimeZone(DateTimeZone.forID(parameters.getTimezone()))
                                    .withStartingYear(parameters.getStartingYear()));
                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Window unit: " + parameters.getUnit() + " is not supported!");
                        }
                    }
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Window type: " + parameters.getType() + " is not supported!");
                }
            }

            if(parameters.getTrigger() != null) {
                window = window.triggering(getTrigger(parameters.getTrigger()));
            }

            if(parameters.getAllowedLateness() != null) {
                window = window.withAllowedLateness(getDuration(parameters.getUnit(), parameters.getAllowedLateness()));
            }

            if(parameters.getDiscardingFiredPanes() != null) {
                if(parameters.getDiscardingFiredPanes()) {
                    window = window.discardingFiredPanes();
                } else {
                    window = window.accumulatingFiredPanes();
                }
            }

            if(parameters.getTimestampCombiner() != null) {
                window = window.withTimestampCombiner(parameters.getTimestampCombiner());
            }

            return input.apply("WithWindow", window);
        }

        private void validate() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("Window module parameter missing!");
            }
            if(this.parameters.getType() == null) {
                throw new IllegalArgumentException("Window module required type parameter!");
            }
            if(Type.fixed.equals(parameters.getType())
                    && parameters.getSize() == null) {
                throw new IllegalArgumentException("Window module. fixed window requires size!");
            }
            if(Type.sliding.equals(parameters.getType())
                    && (parameters.getSize() == null || parameters.getFrequency() == null)) {
                throw new IllegalArgumentException("Window module. sliding window requires both size and frequency!");
            }
            if(Type.session.equals(parameters.getType())
                    && parameters.getGap() == null) {
                throw new IllegalArgumentException("Window module. session window requires gap!");
            }
        }

        private void setDefaultParameters() {
            if(parameters.getUnit() == null) {
                parameters.setUnit(Unit.second);
            }
            if(parameters.getTimezone() == null) {
                parameters.setTimezone("UTC");
            }
            if(parameters.getStartingYear() == null) {
                parameters.setStartingYear(1970);
            }
            if(parameters.getStartingMonth() == null) {
                parameters.setStartingMonth(1);
            }
            if(parameters.getStartingDay() == null) {
                parameters.setStartingDay(1);
            }
            if(parameters.getOffset() == null) {
                parameters.setOffset(0L);
            }
        }

        private static Duration getDuration(final Unit unit, final Long value) {
            switch (unit) {
                case second: {
                    return Duration.standardSeconds(value);
                }
                case minute: {
                    return Duration.standardMinutes(value);
                }
                case hour: {
                    return Duration.standardHours(value);
                }
                case day: {
                    return Duration.standardDays(value);
                }
                default: {
                    throw new IllegalArgumentException("Illegal window unit: " + unit);
                }
            }
        }

        public static Trigger getTrigger(final TriggerParameter parameter) {
            final Trigger trigger;
            switch (parameter.getType()) {
                case afterWatermark: {
                    if(parameter.getEarlyFiringTrigger() != null && parameter.getLateFiringTrigger() != null) {
                        trigger = AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings((Trigger.OnceTrigger) getTrigger(parameter.getEarlyFiringTrigger()))
                                .withLateFirings((Trigger.OnceTrigger) getTrigger(parameter.getLateFiringTrigger()));
                    } else if(parameter.getEarlyFiringTrigger() != null) {
                        trigger = AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings((Trigger.OnceTrigger) getTrigger(parameter.getEarlyFiringTrigger()));
                    } else if(parameter.getLateFiringTrigger() != null) {
                        trigger = AfterWatermark.pastEndOfWindow()
                                .withLateFirings((Trigger.OnceTrigger) getTrigger(parameter.getLateFiringTrigger()));
                    } else {
                        trigger = AfterWatermark.pastEndOfWindow();
                    }
                    break;
                }
                case afterProcessingTime: {
                    final AfterProcessingTime afterProcessingTime = AfterProcessingTime.pastFirstElementInPane();
                    trigger = afterProcessingTime.plusDelayOf(
                            getDuration(parameter.getPastFirstElementDelayUnit(), parameter.getPastFirstElementDelay()));
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
                    for(final TriggerParameter child : parameter.getChildrenTriggers()) {
                        triggers.add(getTrigger(child));
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
                    trigger = Repeatedly.forever(getTrigger(parameter.getForeverTrigger()));
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Not supported window trigger: " + parameter.getType());
                }
            }

            if(parameter.getFinalTrigger() != null) {
                return trigger.orFinally((Trigger.OnceTrigger) getTrigger(parameter.getFinalTrigger()));
            }

            return trigger;
        }

    }

}
