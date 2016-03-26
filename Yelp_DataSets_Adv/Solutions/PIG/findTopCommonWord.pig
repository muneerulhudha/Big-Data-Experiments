shakespereWords = LOAD '/home/hduser/UJG/Assignment_6/Shakespeare';
bibleWords = LOAD '/home/hduser/UJG/Assignment_6/Bible';

tokenizedShakesWords = FOREACH shakespereWords GENERATE FLATTEN(TOKENIZE((chararray)$0)) as word;
lengthTokenShakesWords = FILTER tokenizedShakesWords BY SIZE(word) == (int)'$len';
groupTokenWords = GROUP lengthTokenShakesWords BY word;
wordCountShakes = FOREACH groupTokenWords GENERATE COUNT(lengthTokenShakesWords) as wCount, group as word;
sortedShakesWC = ORDER wordCountShakes BY wCount DESC;
top10Shakes = limit sortedShakesWC 10;
shakesShall =  FILTER sortedShakesWC by word == 'shall';


tokenizedBibleWords = FOREACH bibleWords GENERATE FLATTEN(TOKENIZE((chararray)$0)) as word;
lengthTokenBibleWords = FILTER tokenizedBibleWords BY SIZE(word) == (int)'$len';
groupTokenWords = GROUP lengthTokenBibleWords BY word;
wordCountBible = FOREACH groupTokenWords GENERATE COUNT(lengthTokenBibleWords) as wCount, group as word;
sortedBibleWC = ORDER wordCountBible BY wCount DESC;
top10Bible = limit sortedBibleWC 10;

joinData = JOIN sortedShakesWC BY word, sortedBibleWC BY word;
sumJoinData = FOREACH joinData GENERATE sortedBibleWC::word as word, (sortedShakesWC::wCount + sortedBibleWC::wCount) as wCount;
sortedSumJoinData = ORDER sumJoinData BY wCount DESC;
top10Join = limit sortedSumJoinData 10;
DUMP top10Join;