register Part4_2.jar;

define BagGroup datafu.pig.bags.BagGroup();

sampleDatas = LOAD '$input' USING PigStorage(',');
geneDatas = FOREACH sampleDatas GENERATE $0 AS sampleID, TOTUPLE($1..) AS geneValues:tuple();

parsedGenes = FOREACH geneDatas GENERATE Part4_2(geneValues) as geneValues;
groupGenes = FOREACH parsedGenes GENERATE FLATTEN(geneValues) as geneValues;

grouped = GROUP groupGenes BY geneValues;
cleanGroup = FOREACH grouped GENERATE $1;

--filtered = FILTER grouped BY $1.

STORE cleanGroup INTO '$output' USING PigStorage(',');

