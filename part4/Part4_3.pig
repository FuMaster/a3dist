--register Part4_3.jar;
sampleDatas = LOAD '$input' USING PigStorage(',');

geneBag1 = FOREACH sampleDatas GENERATE $0 AS sampleID1, (bag{tuple()})TOBAG(TOTUPLE($1..)) AS geneValues1:bag{t:tuple()};

geneBag2 = FOREACH sampleDatas GENERATE $0 AS sampleID2, (bag{tuple()})TOBAG(TOTUPLE($1..)) AS geneValues2:bag{t:tuple()};

--rankedGenes = RANK geneDatas;
geneX = CROSS geneBag1, geneBag2;
geneX2 = FILTER geneX BY geneBag1::sampleID1 > geneBag2::sampleID2; --sampleID 1 and sampleID 2

--DUMP highestValues;
STORE geneX INTO '$output' USING PigStorage(',');
