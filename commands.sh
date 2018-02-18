bash /home/ponny/hadoop/bin/start-all.sh
# go to the diretory of the java file and then run this command
hadoop com.sun.tools.javac.Main Customer.java
# Then create a jar file for hadoop to run
jar -cf cus.jar Customer*.class
# Then run the hadoop jar file using the command:
hadoop jar cus.jar Customer /15BCE1257/Lab_6/input /15BCE1257/Lab_6/ques1/cus_output
