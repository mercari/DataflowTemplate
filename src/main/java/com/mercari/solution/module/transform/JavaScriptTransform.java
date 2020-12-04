package com.mercari.solution.module.transform;

import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaScriptTransform {

    public static <ElementT> Transform<ElementT> transform(
            final ValueProvider<String> script,
            final ValueProvider<String> fieldFuncMap,
            final ValueProvider<Boolean> skipScriptError,
            final ValueProvider<Boolean> hideScriptError,
            final ScriptExecutor<ElementT> executor) {
        return new Transform(script, fieldFuncMap, skipScriptError, hideScriptError, executor);
    }

    public static class Transform<ElementT> extends PTransform<PCollection<ElementT>, PCollectionTuple> {

        public final TupleTag<ElementT> tagMain  = new TupleTag<ElementT>(){ private static final long serialVersionUID = 1L; };
        public final TupleTag<ElementT> tagError = new TupleTag<ElementT>(){ private static final long serialVersionUID = 1L; };

        private static final Logger LOG = LoggerFactory.getLogger(Transform.class);

        private final ValueProvider<String> fieldFunc;
        private final ValueProvider<String> script;
        private final ValueProvider<Boolean> skipScriptError;
        private final ValueProvider<Boolean> hideScriptError;
        private final ScriptExecutor<ElementT> executor;

        private Transform(final ValueProvider<String> script,
                          final ValueProvider<String> fieldFunc,
                          final ValueProvider<Boolean> skipScriptError,
                          final ValueProvider<Boolean> hideScriptError,
                          final ScriptExecutor<ElementT> executor) {
            this.script = script;
            this.fieldFunc = fieldFunc;
            this.skipScriptError = skipScriptError;
            this.hideScriptError = hideScriptError;
            this.executor = executor;
        }

        @Override
        public PCollectionTuple expand(PCollection<ElementT> elements) {
            return elements.apply("ExecuteJS", ParDo.of(new DoFn<ElementT, ElementT>() {

                private transient Invocable invocable;
                private transient Map<String,String> fieldFuncMap;
                private transient Boolean skipScriptErrorFlag;
                private transient Boolean hideScriptErrorFlag;

                @Setup
                public void setup() throws ScriptException {
                    if(script.get() == null || fieldFunc.get() == null || !fieldFunc.get().contains(":")) {
                        this.invocable = null;
                        LOG.info("Not use JSTransform");
                    } else {
                        String scriptText = script.get();
                        if(scriptText.startsWith("gs://")) {
                            scriptText = StorageUtil.readString(scriptText);
                            LOG.info("scriptText");
                            LOG.info(scriptText);
                        }
                        final ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
                        engine.eval(scriptText);
                        this.invocable = (Invocable)engine;
                        this.fieldFuncMap = Arrays.stream(fieldFunc.get().split(","))
                                .map(s -> s.split(":"))
                                .filter(s -> s.length == 2)
                                .collect(Collectors.toMap(s -> s[0].trim(), s-> s[1].trim()));
                    }
                    this.skipScriptErrorFlag = skipScriptError.get();
                    this.hideScriptErrorFlag = hideScriptError.get();
                }

                @ProcessElement
                public void processElement(ProcessContext c) throws ScriptException, NoSuchMethodException {
                    final ElementT element = c.element();
                    if(this.invocable == null) {
                        c.output(element);
                        return;
                    }
                    try {
                        final ElementT output = executor.execute(element, this.invocable, this.fieldFuncMap);
                        c.output(output);
                    } catch (Exception e) {
                        if(!this.skipScriptErrorFlag) {
                            if(hideScriptErrorFlag && e instanceof ScriptException) {
                                //throw new ScriptException("JSTransform ScriptException");
                            }
                            throw e;
                        } else {
                            if(e instanceof ScriptException) {
                                LOG.warn(String.format("JSTransform ScriptException: %s", hideScriptErrorFlag ? "" : e.getMessage()));
                            } else if(e instanceof NoSuchMethodException) {
                                LOG.warn(String.format("JSTransform NoSuchMethodException: %s", e.getMessage()));
                            } else {
                                LOG.warn(String.format("JSTransform Script Exception: %s", e.getMessage()));
                            }
                            c.output(tagError, element);
                        }
                    }
                }
            }).withOutputTags(tagMain, TupleTagList.of(tagError)));
        }
    }

    public interface ScriptExecutor<ElementT> extends Serializable {
        ElementT execute(ElementT element,
                         Invocable invocable,
                         Map<String,String> fieldFuncMap) throws ScriptException, NoSuchMethodException;
    }

}
