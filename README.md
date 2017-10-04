# hadoop_map_reduce

I suppose that each line in input file is a transaction. 

TokenizerMapper extends Mapper function.

In the map(), I try to count word
If the key not in the hash map, we add(Key,1) into the hash map
else, we put the (key,count+1) into the hash map
After we have already got the hash map for one customer's transaction, we use a loop to send<item, <other_item,count>>



HashCombiner extends Reducer.

It is used to do the combination after map function. That means, for those have the same value of key, we will try to combine the mapwritable object, when there is two mapwritable object, if they have the same key, we will add the two count.


IntSumReducer extends reducer

It will transform the mapwritable object into map object. We will sort the map object according to the value. We create a results to store the order of the item. We will transform the string into Text object. Finally, we output the (key, output).



My input:
  
file01 :  
book1, book2, cd1  
book1, cd2  
  
file02:  
book1, book2, cd3  
book1, book2, cd2  
  
My output:

par-r-00000:
(it's already sorted, though i didn't show the exact number to customer)

book1	       book2 cd2 cd1 cd3  
book2	       book1 cd2 cd1 cd3  
cd1	         book2 book1  
cd2	         book1 book2  
cd3	         book2 book1  

