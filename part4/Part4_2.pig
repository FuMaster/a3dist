register Part4_2.jar;
sampleDatas = LOAD '$input' USING PigStorage(',');
-- AS (sampleID:chararray, geneValues:chararray); --reads in a line of input
geneDatas = FOREACH sampleDatas GENERATE $0 AS sampleID, TOTUPLE($1..) AS geneValues:tuple();

--rankedGenes = RANK geneDatas;
highestValues = FOREACH geneDatas GENERATE sampleID, Part4_2(geneValues);

--DUMP highestValues;
STORE highestValues INTO '$output' USING PigStorage(',');
