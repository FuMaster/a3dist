register Part4_3.jar;
sampleDatas = LOAD '$input' USING PigStorage(',');
-- AS (sampleID:chararray, geneValues:chararray); --reads in a line of input
geneDatas = FOREACH sampleDatas GENERATE $0 AS sampleID, TOTUPLE($1..) AS geneValues:tuple();

--rankedGenes = RANK geneDatas;
geneTuples = FOREACH geneDatas GENERATE sampleID, Part4_3(geneValues);

--DUMP highestValues;
STORE highestValues INTO '$output' USING PigStorage(',');
