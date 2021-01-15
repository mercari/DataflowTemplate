package com.mercari.solution.config;

import com.google.gson.*;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Config implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    private static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormat.forPattern("yyyyMMdd");
    private static final String[] RESERVED_PARAMETERS = {
            "__EVENT_EPOCH_SECOND__",
            "__EVENT_EPOCH_SECOND_PRE__",
            "__EVENT_EPOCH_MILLISECOND__",
            "__EVENT_EPOCH_MILLISECOND_PRE__",
            "__EVENT_DATETIME_ISO__",
            "__EVENT_DATETIME_ISO_PRE__"
    };

    private String name;
    private Settings settings;
    private List<SourceConfig> sources;
    private List<TransformConfig> transforms;
    private List<SinkConfig> sinks;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Settings getSettings() {
        return settings;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public List<SourceConfig> getSources() {
        return sources;
    }

    public void setSources(List<SourceConfig> sources) {
        this.sources = sources;
    }

    public List<TransformConfig> getTransforms() {
        return transforms;
    }

    public void setTransforms(List<TransformConfig> transforms) {
        this.transforms = transforms;
    }

    public List<SinkConfig> getSinks() {
        return sinks;
    }

    public void setSinks(List<SinkConfig> sinks) {
        this.sinks = sinks;
    }

    public boolean isCalcitePlanner() {
        if(this.transforms == null) {
            return false;
        }
        final Optional<TransformConfig> tc = this.transforms.stream()
                .filter(c -> TransformConfig.Module.beamsql.equals(c.getModule()))
                .findFirst();
        if(!tc.isPresent()) {
            return false;
        }
        if(tc.get().getParameters() == null) {
            return false;
        }
        if(!tc.get().getParameters().has("planner")) {
            return false;
        }
        if(!tc.get().getParameters().get("planner").isJsonPrimitive()) {
            return false;
        }

        return "calcite".equals(tc.get().getParameters().get("planner").getAsString().trim().toLowerCase());
    }

    public static Config parse(final String configJson, final String[] args) throws Exception {
        final Map<String, Map<String, String>> argsParameters = filterConfigArgs(args);
        final String templatedConfigJson = executeTemplate(
                configJson, argsParameters.getOrDefault("template", new HashMap<>()));

        final JsonObject jsonObject;
        try {
            jsonObject= new Gson().fromJson(templatedConfigJson, JsonObject.class);
        } catch (Exception e) {
            final String errorMessage = "Failed to parse template json: " + templatedConfigJson;
            LOG.error(errorMessage);
            throw new IllegalArgumentException(errorMessage, e);
        }

        // Config sources parameters
        if(!jsonObject.has("sources")) {
            throw new IllegalArgumentException("Config must has sources!");
        }
        final JsonElement sources = jsonObject.get("sources");
        if(!sources.isJsonArray()) {
            throw new IllegalArgumentException("Config sources must be array! : " + sources.toString());
        }
        replaceParameters(sources.getAsJsonArray(), argsParameters);

        // Config transforms parameters
        if(jsonObject.has("transforms")) {
            final JsonElement transforms = jsonObject.get("transforms");
            if(!transforms.isJsonArray()) {
                throw new IllegalArgumentException("Config transforms must be array! : " + transforms.toString());
            }
            replaceParameters(transforms.getAsJsonArray(), argsParameters);
        }

        // Config sinks parameters
        if(jsonObject.has("sinks")) {
            final JsonElement sinks = jsonObject.get("sinks");
            if(!sinks.isJsonArray()) {
                throw new IllegalArgumentException("Config sinks must be array! : " + sinks.toString());
            }
            replaceParameters(sinks.getAsJsonArray(), argsParameters);
        }

        LOG.info("Pipeline config: \n" + new GsonBuilder().setPrettyPrinting().create().toJson(jsonObject));
        final String jsonText = replaceParameters(jsonObject.toString());
        try {
            final Config config = new Gson().fromJson(jsonText, Config.class);
            if(config == null) {
                throw new IllegalArgumentException("Json Config must not be null !");
            }
            if(config.getSources() == null || config.getSources().size() == 0) {
                throw new IllegalArgumentException("Inputs must not be null or size zero !");
            }
            config.setSources(config.getSources().stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
            if(config.getTransforms() == null) {
                config.setTransforms(new ArrayList<>());
            }
            config.setTransforms(config.getTransforms().stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
            if(config.getSinks() == null) {
                config.setSinks(new ArrayList<>());
            }
            config.setSinks(config.getSinks().stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
            return config;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to parse config json: " + jsonText, e);
        }
    }

    private static void replaceParameters(final JsonArray array, final Map<String, Map<String, String>> args) {
        for(final JsonElement element : array) {
            if(element.isJsonNull()) {
                continue;
            }
            if(!element.isJsonObject()) {
                throw new IllegalArgumentException("Config module must be object! -> " + element.toString());
            }
            final JsonObject module = element.getAsJsonObject();
            if(!module.has("name")) {
                throw new IllegalArgumentException("Config module must be set name -> " + module.toString());
            }
            final String name = module.get("name").getAsString();
            if(!args.containsKey(name)) {
                continue;
            }
            if(!module.has("parameters")) {
                module.add("parameters", new JsonObject());
            }

            final JsonObject parameters = module.get("parameters").getAsJsonObject();
            for(final Map.Entry<String, String> arg : args.get(name).entrySet()) {
                parameters.addProperty(arg.getKey(), arg.getValue());
            }
        }
    }

    private static String replaceParameters(final String json) {
        final LocalDate today = LocalDate.now(DateTimeZone.forID("Asia/Tokyo"));
        return json
                .replaceAll("<TODAY>", FORMATTER_YYYYMMDD.print(today))
                .replaceAll("<YESTERDAY>", FORMATTER_YYYYMMDD.print(today.plusDays(-1)));
    }

    private static Map<String, Map<String, String>> filterConfigArgs(final String[] args) {
        return Arrays.stream(args)
                .filter(s -> s.contains("=") && s.split("=")[0].contains("."))
                .map(s -> s.startsWith("--") ? s.replaceFirst("--", "") : s)
                .collect(Collectors.groupingBy(
                        s -> s.substring(0, s.indexOf(".")),
                        Collectors.toMap(
                                s -> s.substring(s.indexOf(".") + 1).split("=")[0],
                                s -> s.substring(s.indexOf(".") + 1).split("=", 2)[1],
                                (s1, s2) -> s2)));
    }

    private static String executeTemplate(final String config, final Map<String, String> parameters) throws IOException, TemplateException {
        final Map<String, Object> map = new HashMap<>();
        for(final String reservedParameter : RESERVED_PARAMETERS) {
            map.put(reservedParameter, String.format("${%s}", reservedParameter));
        }
        for(final Map.Entry<String, String> entry : parameters.entrySet()) {
            JsonElement jsonElement;
            try {
                jsonElement = new Gson().fromJson(entry.getValue(), JsonElement.class);
            } catch (final JsonSyntaxException e) {
                jsonElement = new JsonPrimitive(entry.getValue());
            }
            map.put(entry.getKey(), extractTemplateParameters(jsonElement));
        }

        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_30);
        templateConfig.setNumberFormat("computer");
        final Template template = new Template("config", new StringReader(config), templateConfig);
        final StringWriter stringWriter = new StringWriter();
        template.process(map, stringWriter);
        return stringWriter.toString();
    }

    private static Object extractTemplateParameters(final JsonElement jsonElement) {
        if(jsonElement.isJsonPrimitive()) {
            if(jsonElement.getAsJsonPrimitive().isBoolean()) {
                return jsonElement.getAsBoolean();
            } else if(jsonElement.getAsJsonPrimitive().isString()) {
                return jsonElement.getAsString();
            } else if(jsonElement.getAsJsonPrimitive().isNumber()) {
                return jsonElement.getAsLong();
            }
            return jsonElement.toString();
        }
        if(jsonElement.isJsonObject()) {
            final Map<String, Object> map = new HashMap<>();
            jsonElement.getAsJsonObject().entrySet().forEach(kv -> map.put(kv.getKey(), extractTemplateParameters(kv.getValue())));
            return map;
        }
        if(jsonElement.isJsonArray()) {
            final List<Object> list = new ArrayList<>();
            jsonElement.getAsJsonArray().forEach(element -> list.add(extractTemplateParameters(element)));
            return list;
        }
        return null;
    }

}
