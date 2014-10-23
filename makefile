JFLAGS = -g -classpath $(HADOOP_CLASSPATH)'/*:.' 
JC = javac 
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	project1/daniefer/ArticleCounter.java \
	project1/daniefer/FiveMostCommonCounter.java \
	project1/Main.java 

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) ./project1/*.class
	$(RM) ./project1/daniefer/*.class
	$(RM) ./dan.ferguson.jar
jar:
	jar -cvf dan.ferguson.jar ./project1/*.class ./project1/daniefer/*.class
