# Exercise 15: 
It is the same like **ex_3**, but having unique in value

In this exercise, when implementing MapReduce with multiple file txt make it is not easily handle because using List<String> only save all elements through each file txt and will not have any elements when go next to new file. So I think the easy way to handle it is group all file txt.

**Method Group File**: In folder test/test.ipynb

Besides, I have an idea if not implementing group file, it is implementing 2 job map-reduce but it need to be save the result of job 1 to implement for job 2 (It is complicated) 
## Format input: 
Data in multiple texts. The content of each documents contains only lowercase characters, spaces and newline characters.
## Content: 
=> (numchar,numword), with **numchar** is the number of characters and **numword** is the number of **unique** word
## Example  
**Sample dataset:** 
![Sample Ex 15](/images/Sample_Ex_13.png)
=> **Result**:        
![Paraphrase Ex 15](/images/Paraphrase_Ex_15.png)


