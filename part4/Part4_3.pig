register Part4_3.jar;
sampleDatas = LOAD '$input' USING PigStorage(',');

geneBag1 = FOREACH sampleDatas GENERATE $0 AS sampleID1, (bag{tuple()})TOBAG(TOTUPLE($1..)) AS geneValues1:bag{t:tuple()};

geneBag2 = FOREACH sampleDatas GENERATE $0 AS sampleID2, (bag{tuple()})TOBAG(TOTUPLE($1..)) AS geneValues2:bag{t:tuple()};

--rankedGenes = RANK geneDatas;
geneX = CROSS geneBag1, geneBag2;
geneX2 = FILTER geneX BY geneBag1::sampleID1 > geneBag2::sampleID2;

--temp = FOREACH geneX2 GENERATE TOTUPLE($0..) as name:tuple();

pair = FOREACH geneX2 GENERATE Part4_3(TOTUPLE($0..));
--pair = FOREACH geneX2 GENERATE Part4_3($0..);
--DUMP highestValues;
STORE pair INTO '$output' USING PigStorage(',');
