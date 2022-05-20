package com.mercari.solution.util.domain.text.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TokenAnalyzerTest {

    @Test
    public void testExtractHtmlTags() {
        final TokenAnalyzer.TokenizerConfig tokenizerConfig = new TokenAnalyzer.TokenizerConfig();
        final List<TokenAnalyzer.TokenFilterConfig> tokenFilterConfigs = new ArrayList<>();
        final List<TokenAnalyzer.CharFilterConfig> charFilterConfigs = new ArrayList<>();

        final TokenAnalyzer.CharFilterConfig charFilterHtml = new TokenAnalyzer.CharFilterConfig();
        charFilterHtml.setType(TokenAnalyzer.CharFilterType.HTMLStripCharFilter);
        charFilterHtml.setEscapedTags(Set.of("a","title"));
        charFilterHtml.setDefaults();
        charFilterConfigs.add(charFilterHtml);

        tokenizerConfig.setType(TokenAnalyzer.TokenizerType.PatternTokenizer);
        tokenizerConfig.setPattern("(<title(?: .+?)?>.*?<\\/title>)|(<a(?: .+?)?>.*?<\\/a>)|(https?://[\\w!\\?/\\+\\-_~=;\\.,\\*&@#\\$%\\(\\)'\\[\\]]+)|([\\w\\-\\._]+@[\\w\\-\\._]+\\.[A-Za-z]+)");
        tokenizerConfig.setGroup(0);
        tokenizerConfig.setDefault();

        final TokenAnalyzer.TokenFilterConfig typingFilter = new TokenAnalyzer.TokenFilterConfig();
        typingFilter.setType(TokenAnalyzer.TokenFilterType.PatternTypingFilter);

        var rule0 = new TokenAnalyzer.TokenFilterConfig.PatternTypingRuleParameter();
        rule0.setPattern("<a .*?>(.*?)<\\/a>");
        rule0.setTypeTemplate("anchor:$1");
        rule0.setDefault();
        var rule1 = new TokenAnalyzer.TokenFilterConfig.PatternTypingRuleParameter();
        rule1.setPattern("[\\w\\-\\._]+@[\\w\\-\\._]+\\.[A-Za-z]+");
        rule1.setTypeTemplate("mail");
        rule1.setDefault();
        var rule2 = new TokenAnalyzer.TokenFilterConfig.PatternTypingRuleParameter();
        rule2.setPattern("https?://[\\w!\\?/\\+\\-_~=;\\.,\\*&@#\\$%\\(\\)'\\[\\]]+");
        rule2.setTypeTemplate("url");
        rule2.setDefault();
        var rule3 = new TokenAnalyzer.TokenFilterConfig.PatternTypingRuleParameter();
        rule3.setPattern("<title(?: .+?)?>(.*?)<\\/title>");
        rule3.setTypeTemplate("title");
        rule3.setDefault();

        typingFilter.setPatternTypingRules(Arrays.asList(rule0, rule1, rule2, rule3));
        typingFilter.setDefaults();
        tokenFilterConfigs.add(typingFilter);

        final TokenAnalyzer.TokenFilterConfig replaceFilter = new TokenAnalyzer.TokenFilterConfig();
        replaceFilter.setType(TokenAnalyzer.TokenFilterType.PatternCaptureGroupTokenFilter);
        replaceFilter.setPatterns(Arrays.asList("<title(?: .+?)?>(.*?)<\\/title>", "<a href=\"(.*?)\".*?>.*?<\\/a>"));
        replaceFilter.setPreserveOriginal(false);
        replaceFilter.setDefaults();
        tokenFilterConfigs.add(replaceFilter);

        final Analyzer analyzer = new TokenAnalyzer(charFilterConfigs, tokenizerConfig, tokenFilterConfigs);
        final String text = "<html><head><title>This is title</title></head><body><!-- comment@example.com \n --><img src=\"https://example.com/image.jpg\" /><p>ok<p> <a href=\"https://example.com/anchor\">Anchor text</a> <p>こちらにアクセスください -> https://example.com/link?p=ok</p> メールならこちらです -> support@example.com 24時間受け付けています. <p>お電話はこちらです -> <span>090-0000-0000</span></p></body></html>";
        try(final TokenStream tokenStream = analyzer.tokenStream("field", new StringReader(text))) {
            tokenStream.reset();
            int count = 0;
            while (tokenStream.incrementToken()) {
                final CharTermAttribute cta = tokenStream.getAttribute(CharTermAttribute.class);
                final TypeAttribute ta = tokenStream.getAttribute(TypeAttribute.class);
                count += 1;
                if("title".equals(ta.type())) {
                    Assert.assertEquals("This is title", cta.toString());
                } else if("url".equals(ta.type())) {
                    Assert.assertEquals("https://example.com/link?p=ok", cta.toString());
                } else if("mail".equals(ta.type())) {
                    Assert.assertEquals("support@example.com", cta.toString());
                } else {
                    Assert.assertEquals("anchor:Anchor text", ta.type());
                    Assert.assertEquals("https://example.com/anchor", cta.toString());
                }
            }
            Assert.assertEquals(4, count);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
