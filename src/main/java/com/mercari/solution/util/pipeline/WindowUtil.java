package com.mercari.solution.util.pipeline;

import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.transforms.windowing.*;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class WindowUtil {

    public static class WindowParameters implements Serializable {

        private WindowType type;
        private DateTimeUtil.TimeUnit unit;
        private Long size;
        private Long period;
        private Long gap;
        private Long offset;
        private String timezone;
        private String startDate;
        private Long allowedLateness;
        private TimestampCombiner timestampCombiner;


        public WindowType getType() {
            return type;
        }

        public void setType(WindowType type) {
            this.type = type;
        }

        public DateTimeUtil.TimeUnit getUnit() {
            return unit;
        }

        public void setUnit(DateTimeUtil.TimeUnit unit) {
            this.unit = unit;
        }

        public Long getSize() {
            return size;
        }

        public void setSize(Long size) {
            this.size = size;
        }

        public Long getPeriod() {
            return period;
        }

        public void setPeriod(Long period) {
            this.period = period;
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

        public String getTimezone() {
            return timezone;
        }

        public void setTimezone(String timezone) {
            this.timezone = timezone;
        }

        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public Long getAllowedLateness() {
            return allowedLateness;
        }

        public void setAllowedLateness(Long allowedLateness) {
            this.allowedLateness = allowedLateness;
        }

        public TimestampCombiner getTimestampCombiner() {
            return timestampCombiner;
        }

        public void setTimestampCombiner(TimestampCombiner timestampCombiner) {
            this.timestampCombiner = timestampCombiner;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(!WindowType.global.equals(this.type)) {
                if(this.size == null) {
                    if(WindowType.fixed.equals(this.type) || WindowType.sliding.equals(this.type)) {
                        errorMessages.add("Aggregation module.window requires size for fixed or sliding window");
                    }
                }
                if(this.period == null) {
                    if(WindowType.sliding.equals(this.type)) {
                        errorMessages.add("Aggregation module.window requires period for sliding window");
                    }
                }
                if(WindowType.session.equals(this.type)) {
                    if(this.gap == null) {
                        errorMessages.add("Aggregation module.window requires gap for session window");
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.type == null) {
                this.type = WindowType.global;
            }
            if(this.unit == null) {
                this.unit = DateTimeUtil.TimeUnit.second;
            }
            if(this.timezone == null) {
                this.timezone = "UTC";
            }
            if(this.startDate == null) {
                this.startDate = "1970-01-01";
            }
            if(this.offset == null) {
                this.offset = 0L;
            }
            if(this.allowedLateness == null) {
                this.allowedLateness = 0L;
            }
        }
    }

    public enum WindowType implements Serializable {
        global,
        fixed,
        sliding,
        session,
        calendar
    }

    public enum AccumulationMode {
        discarding,
        accumulating
    }

    public static <T> Window<T> createWindow(final WindowParameters windowParameters) {
        Window<T> window;
        switch (windowParameters.getType()) {
            case global: {
                window = Window.into(new GlobalWindows());
                break;
            }
            case fixed: {
                window = Window.into(FixedWindows
                        .of(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getSize()))
                        .withOffset(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getOffset())));
                break;
            }
            case sliding: {
                window = Window.into(SlidingWindows
                        .of(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getSize()))
                        .every(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getPeriod()))
                        .withOffset(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getOffset())));
                break;
            }
            case session: {
                window = Window.into(Sessions
                        .withGapDuration(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getGap())));
                break;
            }
            case calendar: {
                final LocalDate startDate = parseStartDate(windowParameters.getStartDate());
                switch (windowParameters.getUnit()) {
                    case day: {
                        window = Window.into(CalendarWindows
                                .days(windowParameters.getSize().intValue())
                                .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                .withStartingDay(startDate.getYear(), startDate.getMonthValue(), startDate.getDayOfMonth()));
                        break;
                    }
                    case week: {
                        window = Window.into(CalendarWindows
                                .weeks(windowParameters.getSize().intValue(), windowParameters.getOffset().intValue())
                                .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                .withStartingDay(startDate.getYear(), startDate.getMonthValue(), startDate.getDayOfMonth()));
                        break;
                    }
                    case month: {
                        window = Window.into(CalendarWindows
                                .months(windowParameters.getSize().intValue())
                                .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                .withStartingMonth(startDate.getYear(), startDate.getMonthValue()));
                        break;
                    }
                    case year: {
                        window = Window.into(CalendarWindows
                                .years(windowParameters.getSize().intValue())
                                .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                .withStartingYear(startDate.getYear()));
                        break;
                    }
                    case second:
                    case minute:
                    case hour:
                    default:
                        throw new IllegalArgumentException("Not supported calendar timeunit type: " + windowParameters.getType());
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Not supported window type: " + windowParameters.getType());
        }

        return window;
    }

    private static LocalDate parseStartDate(final String startDate) {
        final Integer year;
        final Integer month;
        final Integer day;
        if(startDate != null) {
            final String[] s = startDate.split("-");
            if(s.length == 1) {
                year = Integer.valueOf(s[0]);
                month = 1;
                day = 1;
            } else if(s.length == 2) {
                year = Integer.valueOf(s[0]);
                month = Integer.valueOf(s[1]);
                day = 1;
            } else if(s.length == 3) {
                year = Integer.valueOf(s[0]);
                month = Integer.valueOf(s[1]);
                day = Integer.valueOf(s[2]);
            } else {
                year = 1970;
                month = 1;
                day = 1;
            }
        } else {
            year = 1970;
            month = 1;
            day = 1;
        }

        return LocalDate.of(year, month, day);
    }

}
