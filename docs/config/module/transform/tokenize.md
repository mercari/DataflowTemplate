# Tokenize Transform Module (Experimental)

Tokenize transform module tokenizes and processes input text.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `tokenize` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## Tokenize transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| fields | required | Array<Field\> | Define the tokenizing process for each field of the input record. |


## Field parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Specify the name of the output field for the tokenize result.  |
| input | required | String | Specify the field name of the input record you wish to tokenize. The field specified here must be a string type. |
| tokenizer | required | Tokenizer | Define the settings of the tokenizer. |
| charFilters | optional | Array<CharFilter\> | Define the pre-processing to be handled by the tokenizer. |
| filters | optional | Array<TokenFilter\> | Define the post-processing handled by the tokenizer. |

## Tokenizer parameters

| parameter | optional | type | target tokenizer | description |
| --- | --- | --- | --- | --- |
| type | required | Enum | All | Specify the tokenizer name from the list of `Supported Tokenizers` below. |
| userDictionary | optional | String | [JapaneseTokenizer](https://lucene.apache.org/core/8_11_0/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseTokenizer.html) | Specify user dictionary gcs path. |
| mode | optional | Enum | [JapaneseTokenizer](https://lucene.apache.org/core/8_11_0/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseTokenizer.html) | Specify tokenization mode. This determines how the tokenizer handles compound and unknown words. `NORMAL`, `EXTENDED` or `SEARCH`. The default is `NORMAL` |
| discardPunctuation | optional | Boolean | [JapaneseTokenizer](https://lucene.apache.org/core/8_11_0/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseTokenizer.html) | Specify punctuation tokens should be dropped from the output. The default is false. |
| discardCompoundToken | optional | Boolean | [JapaneseTokenizer](https://lucene.apache.org/core/8_11_0/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseTokenizer.html) | Specify compound tokens should be dropped from the output when tokenization `mode` is not `NORMAL`. The default is false. |
| minGram | optional | Integer | [NGramTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/ngram/NGramTokenizer.html) | the smallest n-gram to generate. The default is 2. |
| maxGram | optional | Integer | [NGramTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/ngram/NGramTokenizer.html) | the largest n-gram to generate. The default is 2. |
| pattern | required | String | [PatternTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternTokenizer.html) | Specify the regular expression to split or extract. |
| group | optional | Integer | [PatternTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternTokenizer.html) | Specify which group to extract into tokens. `group`=-1 (the default) is equivalent to "split". |
| tokenizerModel | required | Integer | [OpenNLPTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPTokenizer.html) | Specify GCS path or url for tokenizer [model files](https://opennlp.apache.org/models.html). |
| sentenceModel | required | Integer | [OpenNLPTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPTokenizer.html) | Specify GCS path or url for sentence detector [model file](https://opennlp.apache.org/models.html). |

## CharFilter parameters

| parameter | optional | type | target filter | description |
| --- | --- | --- | --- | --- |
| type | required | Enum | All | Specify the charFilter name from the list of `Supported CharFilters` below. |
| normMap | required | Map<String,String\> | [MappingCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/charfilter/MappingCharFilter.html) | Specify map of String input to String output.|
| escapedTags | optional | Set<String\> | [HTMLStripCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/charfilter/HTMLStripCharFilter.html) | Specify tags in this set (both start and end tags) will not be filtered out. |
| pattern | required | String | [PatternReplaceCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceCharFilter.html) | Specify regular expression for the target of replace string |
| replacement | optional | String | [PatternReplaceCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceCharFilter.html) | Specify replacement text for matched part |
| normalizeName | optional | Enum | [NormalizeCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-icu/org/apache/lucene/analysis/icu/ICUNormalizer2CharFilter.html) | Specify normalization name. `nfc`,`nfkc` or `nfkc_cf`. The default is `nfkc_cf`.  |
| normalizeMode | optional | Enum | [NormalizeCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-icu/org/apache/lucene/analysis/icu/ICUNormalizer2CharFilter.html) | Specify normalization [modes](https://unicode-org.github.io/icu-docs/apidoc/dev/icu4j/com/ibm/icu/text/Normalizer2.Mode.html). `COMPOSE`,`COMPOSE_CONTIGUOUS`,`DECOMPOSE` or `FCD`. The default is `COMPOSE`. |

## TokenFilter parameters

| parameter | optional | type | target filter | description |
| --- | --- | --- | --- | --- |
| type | required | Enum | All | Specify the filter name from the list of `Supported TokenFilters` below.    |
| min | optional | Integer | [LengthFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/LengthFilter.html), [NGramTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/ngram/NGramTokenFilter.html), [ShingleFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/shingle/ShingleFilter.html) | Specify the minimum number of characters in the token, the number of divisions by NGram, or the number of Shingle joins. |
| max | optional | Integer | [LengthFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/LengthFilter.html), [NGramTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/ngram/NGramTokenFilter.html), [ShingleFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/shingle/ShingleFilter.html) | Specify the maximum number of characters in the token, the number of divisions by NGram, or the number of Shingle joins. |
| useWhiteList | optional | Boolean | [TypeTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/TypeTokenFilter.html) | Specify if you want to keep the specified tag rather than remove it from the token stream |
| words | optional | Array<String\> | [StopFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/StopFilter.html), [KeepWordFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/KeepWordFilter.html) | Specify strings that you want to remove or keep from the token stream |
| tags | optional | Array<String\> | [TypeTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/TypeTokenFilter.html), [JapanesePartOfSpeechStopFilter](https://lucene.apache.org/core/8_11_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapanesePartOfSpeechStopFilter.html) | Specify tags that you want to remove or keep from the token stream |
| outputUnigrams | optional | Boolean | [ShingleFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/shingle/ShingleFilter.html) | Shall the output stream contain the input tokens (unigrams) as well as shingles. The default is true. |
| pattern | optional | String | [PatternReplaceFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceFilter.html) | Specify the regular expression pattern you want to extract |
| replacement | optional | String | [PatternReplaceFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceFilter.html) | Specify a string to replace the part of the pattern that matches |
| replaceAll | optional | Boolean | [PatternReplaceFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceFilter.html) | Specify whether to replace all parts matching the pattern |
| patterns | optional | Array<String\> | [PatternCaptureGroupTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternCaptureGroupTokenFilter.html) | Specify the pattern(s) you wish to extract　|
| patternTypingRules | optional | Array<PatternTypingRule\> | [PatternTypingFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternTypingFilter.html) | Patterns and TypeAttribute responses when a pattern is matched　|
| preserveOriginal | optional | Boolean | [PatternCaptureGroupTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternCaptureGroupTokenFilter.html), [EdgeNGramTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/ngram/EdgeNGramTokenFilter.html), [ASCIIFoldingFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/ASCIIFoldingFilter.html) |　Specify whether to keep strings matching the condition |
| model | optional | String | [OpenNLPChunkerFilter](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPChunkerFilter.html), [OpenNLPLemmatizerFilter](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPLemmatizerFilter.html), [OpenNLPPOSFilter](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPPOSFilter.html) |　Specify the GCS path or URL that contains the OpenNLP model file |

## PatternTypingRule parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| pattern | required | String | Regular expressions used for extraction. |
| typeTemplate | required | String | TypeAttribute expression when a pattern is matched. |


## Supported Tokenizer

| filter | parameters | description |
| --- | --- | --- |
| [StandardTokenizer](https://lucene.apache.org/core/8_11_1/core/org/apache/lucene/analysis/standard/StandardTokenizer.html) | - | A grammar-based tokenizer. This tokenizer implements the Word Break rules from the Unicode Text Segmentation algorithm, as specified in [Unicode Standard Annex #29](https://unicode.org/reports/tr29/). |
| [JapaneseTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseTokenizer.html) | userDictionary, discardPunctuation, discardCompoundToken, mode | Tokenizer for Japanese that uses morphological analysis. |
| [NGramTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/ngram/NGramTokenizer.html) | minGram, maxGram | Tokenizes the input into n-grams of the given size(s). |
| [WhitespaceTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/WhitespaceTokenizer.html) | - | A tokenizer that divides text at whitespace characters as defined by Character.isWhitespace(int). |
| [PatternTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternTokenizer.html) | pattern, group | This tokenizer uses regex pattern matching to construct distinct tokens for the input stream. |
| [SimplePatternTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/SimplePatternTokenizer.html) | pattern | This tokenizer uses a Lucene RegExp or (expert usage) a pre-built determinized Automaton, to locate tokens. |
| [SimplePatternSplitTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/SimplePatternSplitTokenizer.html) | pattern | This tokenizer uses a Lucene RegExp or (expert usage) a pre-built determinized Automaton, to locate tokens. This is just like `SimplePatternTokenizer` except that the pattern should make valid token separator characters, like String.split. |
| [KeywordTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/KeywordTokenizer.html) | - | Emits the entire input as a single token. |
| [URLEmailTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/standard/UAX29URLEmailTokenizer.html) | - | Tokenizer that is the same as `StandardTokenizer` but can extract URLs and email addresses. |
| [OpenNLPTokenizer](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPTokenizer.html) | tokenizerModel, sentenceModel | Run [OpenNLP](https://opennlp.apache.org/) SentenceDetector and Tokenizer. |

## Supported CharFilters

| filter | parameters | description |
| --- | --- | --- |
| [MappingCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/charfilter/MappingCharFilter.html) | normMap | Simplistic CharFilter that applies the mappings contained in a `normMap`. |
| [NormalizeCharFilter](https://lucene.apache.org/core/7_2_1/analyzers-icu/org/apache/lucene/analysis/icu/ICUNormalizer2CharFilter.html) | normalizeName, normalizeMode | Unicode normalization functionality for standard Unicode normalization or for using custom mapping tables. |
| [HTMLStripCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/charfilter/HTMLStripCharFilter.html) | escapedTags | CharFilter that attempts to strip out HTML constructs. |
| [PatternReplaceCharFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceCharFilter.html) | pattern, replacement | CharFilter that uses a regular expression for the target of replace string. |

## Supported TokenFilters

| filter | parameters | description |
| --- | --- | --- |
| [StopFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/StopFilter.html) | words | Removes stop words from a token stream. |
| [KeepWordFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/KeepWordFilter.html) | words | TokenFilter that only keeps tokens with text contained in the required words. This filter behaves like the inverse of StopFilter. |
| [TypeTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/TypeTokenFilter.html) | tags, useWhiteList | Removes tokens whose types appear in a set of blocked types from a token stream. |
| [LengthFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/LengthFilter.html) | min, max | Removes words that are too long or too short from the stream. |
| [LowerCaseFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/LowerCaseFilter.html) | - | Normalizes token text to lower case. |
| [UpperCaseFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/core/UpperCaseFilter.html) | - | Normalizes token text to upper case. |
| [CJKWidthFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/cjk/CJKWidthFilter.html) | - | Normalizes CJK width differences. Folds fullwidth ASCII variants into the equivalent basic latin. Folds halfwidth Katakana variants into the equivalent kana |
| [ASCIIFoldingFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/ASCIIFoldingFilter.html) | preserveOriginal | Converts alphabetic, numeric, and symbolic Unicode characters which are not in the first 127 ASCII characters (the "Basic Latin" Unicode block) into their ASCII equivalents, if one exists.  |
| [PorterStemFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/en/PorterStemFilter.html) | - | Transforms the token stream as per the Porter stemming algorithm. (Note: the input to the stemming filter must already be in lower case) |
| [ShingleFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/shingle/ShingleFilter.html) | min, max, outputUnigrams | ShingleFilter constructs shingles (token n-grams) from a token stream. In other words, it creates combinations of tokens as a single token. |
| [PatternReplaceFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternReplaceFilter.html) | pattern, replacement, replaceAll | TokenFilter which applies a Pattern to each token in the stream, replacing match occurrences with the specified replacement string |
| [PatternCaptureGroupTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternCaptureGroupTokenFilter.html) | patterns, preserveOriginal | CaptureGroup uses Java regexes to emit multiple tokens - one for each capture group in one or more patterns. |
| [PatternTypingFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/pattern/PatternTypingFilter.html) | rules | Set a type attribute to a parameterized value when tokens are matched by any of a several regex patterns. |
| [LimitTokenCountFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/miscellaneous/LimitTokenCountFilter.html) | maxTokenCount | This TokenFilter limits the number of tokens while indexing. |
| [EdgeNGramTokenFilter](https://lucene.apache.org/core/8_11_1/analyzers-common/org/apache/lucene/analysis/ngram/EdgeNGramTokenFilter.html) | min, max, preserveOriginal | Tokenizes the given token into n-grams of given size(s). |
| [JapaneseBaseFormFilter](https://lucene.apache.org/core/8_11_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseBaseFormFilter.html) | - | Replaces term text with the BaseFormAttribute. This acts as a lemmatizer for verbs and adjectives. |
| [JapaneseReadingFormFilter](https://lucene.apache.org/core/8_11_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseReadingFormFilter.html) | useRomaji | TokenFilter that replaces the term attribute with the reading of a token in either katakana or romaji form. The default reading form is katakana. |
| [JapaneseNumberFilter](https://lucene.apache.org/core/8_11_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseNumberFilter.html) | - | TokenFilter that normalizes Japanese numbers (kansūji) to regular Arabic decimal numbers in half-width characters. |
| [JapaneseKatakanaStemFilter](https://lucene.apache.org/core/8_11_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapaneseKatakanaStemFilter.html) | min | Normalizes common katakana spelling variations ending in a long sound character by removing this character (U+30FC). Only katakana words longer than a minimum length are stemmed (default is four). |
| [JapanesePartOfSpeechStopFilter](https://lucene.apache.org/core/8_11_1/analyzers-kuromoji/org/apache/lucene/analysis/ja/JapanesePartOfSpeechStopFilter.html) | tags | Removes tokens that match a set of part-of-speech tags |
| [OpenNLPChunkerFilter](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPChunkerFilter.html) | model | Run OpenNLP chunker. Prerequisite: the OpenNLPTokenizer and OpenNLPPOSFilter must precede this filter. |
| [OpenNLPLemmatizerFilter](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPLemmatizerFilter.html) | model | Runs OpenNLP dictionary-based and/or MaxEnt lemmatizers. |
| [OpenNLPPOSFilter](https://lucene.apache.org/core/8_11_1/analyzers-opennlp/org/apache/lucene/analysis/opennlp/OpenNLPPOSFilter.html) | model | Run OpenNLP POS tagger. Tags all terms in the TypeAttribute. |

## Output field schema

Tokenize results are stored in the following schema for each field specified in parameter `fields`.

| field | type | target | description |
| --- | --- | --- | --- |
| token | String | All | Term text of a token. |
| startOffset | Integer | All | Token's starting offset, the position of the first character corresponding to this token in the source text. |
| endOffset | Integer | All | Token's ending offset, one greater than the position of the last character corresponding to this token in the source text. |
| type | String | All | Token's lexical type. The Default value is `word`. |
| partOfSpeech | String | JapaneseTokenizer | Part of speech of token. |
| inflectionForm | String | JapaneseTokenizer | Inflection form of token |
| inflectionType | String | JapaneseTokenizer | Inflection type of token |
| baseForm | String | JapaneseTokenizer | Base form for inflected adjectives and verbs. |
| pronunciation | String | JapaneseTokenizer | Pronunciation of token |
| reading | String | JapaneseTokenizer | Reading of token |


## Related example config files

* [BigQuery to Tokenize to BigQuery](../../../../examples/bigquery-to-tokenize-to-bigquery.json)
