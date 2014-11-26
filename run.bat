@echo off
echo P7 - GAMMA JOINS
set CLASSPATH=%CLASSPATH%;junit410.jar
set CLASSPATH=%CLASSPATH%;RegTest.jar

echo TESTING READ RELATION
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\ReadRelationTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.ReadRelationTest
pause

echo TESTING BLOOM
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\BloomTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.BloomTest
pause

echo TESTING BLOOM FILTER
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\BloomFilterTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.BloomFilterTest
pause

echo TESTING HJOIN
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\HJoinTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.HJoinTest
pause

echo TESTING HSPLIT
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\HSplitTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.HSplitTest
pause

echo TESTING SPLIT MAP
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\SplitMTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.SplitMTest
pause

echo TESTING MERGE
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\MergeTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.MergeTest
pause

echo TESTING MERGE MAP
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\MergeMTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.MergeMTest
pause

echo TESTING MAP REDUCE BLOOM
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\MapReduceBloomTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.MapReduceBloomTest
pause

echo TESTING MAP REDUCE BLOOM FILTER
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\MapReduceBFilterTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.MapReduceBFilterTest
pause

echo TESTING MAP REDUCE HJOIN
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\MapReduceHJoinTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.MapReduceHJoinTest
pause

echo TESTING HJOIN BLOOM AND FILTER
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\HJoinBloomFiltersTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.HJoinBloomFiltersTest
pause

echo TESTING GAMMA
echo -----------------------------------------------------
javac -cp %CLASSPATH%;src src\tests\GammaTest.java
echo -----------------------------------------------------
java -cp %CLASSPATH%;srcBaseFeature org.junit.runner.JUnitCore tests.GammaTest
pause