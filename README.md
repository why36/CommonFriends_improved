# 金融大数据 作业6
####  吴泓宇 181250155


## 设计思路

对于基本数据类型的情况，我设计了两个MapReduce任务，其中第一个任务将读入的形如

\<person\>, \<friend1\>\<friend2\>…\<friendn\>的数据先通过第一个Mapper转化成

\<friend1\>,\<person>

\<friend2>,\<person>

...

\<friendn>, \<person>的形式

![](figure/4.png)


这之后，通过Hadoop的shuffle，到达Reducer的数据格式为

\<friend1>, \<person1>\<person2>....

\<friend2>, \<person3>...

...

针对第一个Job，我们的Reducer无需对value做多余处理，简单分隔即可，在此我们主要利用了Hadoop中shuffle的特性

![](figure/5.png)



然后数据到达第二个Job，这个Job的Mapper将

\<friend1>, \<person1>\<person2>\<person3>....

转化为

[\<person1>,\<person2>], \<friend1>

[\<person1>,\<person3>], \<friend1>

[\<person2>,\<person3>], \<friend1>

...

![](figure/6.png)


即，将每一个拥有friend n的person两两组合作为key，然后将该friend作为value输出

因此，在Reducer端，经过shuffle之后，收到的数据格式为

[\<person1>,\<person2>], \<friend1> \<friend2> \<friend3>...

[\<person1>,\<person3>], \<friend1>...

...

![](figure/7.png)


同样地，我们很好的利用了shuffle的特性，Reducer只要做一个简单的append输出即可。至此，任务已经完成。


## 实验结果 

![](figure/3.png)

Web页面截图

![](figure/1.png)

![](figure/2.png)

## 总结

在本次实验过程中，由于有实验5的基础，对Hadoop的各种API都有了比较熟练的运用，写起代码来也更加的得心应手，可以将更多思考放在算法的设计上。对于这个项目，得到解决思路的关键在于充分理解Hadoop中shuffle这一操作的作用，shuffle操作本身就带有“取共同”的特性，因此，只需要key，value对设计得当，巧妙利用shuffle特性即可完成。