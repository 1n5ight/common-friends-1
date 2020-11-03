### 一、运行流程和结果

1.将程序打包成jar包，并将friends_list1.txt，friends_list2.txt导入hdfs文件系统下的/user/<username>/input文件夹内

![image](https://github.com/1n5ight/common-friends-1/blob/main/image_folder/1.jpg)

 

2.运行

` hadoop jar common-friends-1.jar `，这是用基本数据类型的程序。用自定义数据类型的jar包名为common-friends-2.jar，运行和common-friends-1.jar一样

![image](https://github.com/1n5ight/common-friends-1/blob/main/image_folder/2.jpg)



 

3.结果输出在/output1/part-r-00000，如下图

![image](https://github.com/1n5ight/common-friends-1/blob/main/image_folder/3.jpg) 

 

 

4.下图证明是在集群上运行的

![image](https://github.com/1n5ight/common-friends-1/blob/main/image_folder/4.jpg) 

### 二、设计思路

​		题目是求出所有用户的共同好友，但是通过观察输入，我发现，一个人A有好友B，但B没有好友A，说明这里的好友是单向的关系。因此，我将整个过程分为两个mapreduce，第一个部分是倒排索引，令输入文件逗号前的值为name，逗号后的值为friend，第一个部分目的是输出所有friend和与其有关连的name
例如输入为
100, 200 300 400 500
200, 100 300 400
300, 100 200 400 500
则第一部分的输出为
100	200, 300
200	100, 300
300	100, 200
400	100, 200, 300
500	100, 300

第二个部分是一一配对，即对第一部分输出的每一个friend所对应的name两两组合，再进行合并，最终输出为
200, 300:	100, 400
100, 300:	200, 500
100, 200:	300, 400

​		在倒排索引部分，比较简单，map部分就是把每一行的name,	friend1 friend2 ... 拆成<name, friend1>，<name, friend2>...，再将key和value交换位置，reduce部分对每个相同的key进行合并，输出结果如下
![image](https://github.com/1n5ight/common-friends-1/blob/main/image_folder/5.jpg)



​		在一一配对部分，map部分要注意的是，必须对倒排索引部分输出的value进行排序，否则，如上图，200那一行按顺序给出的是100, 300，500那一行按顺序给出的是300, 100，但这两个其实是同样一对。排序之后就不会出现这个问题。reduce很简单，根据相同的“name对”进行合并即可，结果如下
![image](https://github.com/1n5ight/common-friends-1/blob/main/image_folder/6.jpg)



​		接着我使用自定义数据类型。我定义了可以包含多个名称的NameList，和表示“name对”的Pair，简化了程序中输入、输出的操作

NameList用于value

```java
package njucs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class NameList implements Writable {
    private String namelist = "";

    public String getList() {
        return namelist;
    }

    public String getName(int i) {
        String[] names = namelist.split(", ");
        return names[i];
    }

    public void addName(String name) {
        StringBuffer sb = new StringBuffer();
        if (namelist.equals("")) {
            sb.append(name);
        } else {
            sb.append(namelist + ", " + name);
        }
        namelist = sb.toString();
    }

    public void readFields(DataInput in) throws IOException {
        namelist = in.readLine();
    }

    public void write(DataOutput out) throws IOException {
        out.writeChars(namelist);
    }
}
```

Pair用于key

```java
package njucs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
    private String name1;
    private String name2;

    public Pair(String s1, String s2) {
        this.name1 = s1;
        this.name2 = s2;
    }

    public String outputPair() {
        return "([" + name1 + ", " + name2 + "]";
    }

    public String getName1() {
        return name1;
    }

    public String getName2() {
        return name2;
    }

    public void readFields(DataInput in) throws IOException {
        name1 = in.readLine();
        name2 = in.readLine();
    }

    public void write(DataOutput out) throws IOException {
        out.writeChars(name1);
        out.writeChars(name2);
    }

    public int compareTo(Pair p) {
        if (name1.compareTo(p.getName1()) < 0) {
            return -1;
        } else if (name1.compareTo(p.getName1()) > 0) {
            return 1;
        } else if (name1.equals(p.getName1()) && name2.compareTo(p.getName2()) < 0) {
            return -1;
        } else if (name1.equals(p.getName1()) && name2.compareTo(p.getName2()) > 0) {
            return 1;
        } else {
            return 0;
        }
    }
}
```



### 三、所遇问题

1. The type Writable is not generic的问题
   		我一开始仿照书上，写的是public class NameList implements Writable<NameList>，但是报错原因是书上这里有问题，Writable不支持泛型，不能加参数，要把后面的<NameList>去掉。

2. String不能使用append等操作
      这个是我对java的操作不了解导致的。Java中操作String一般都是先对StringBuffer进行更改，最后再用toString赋值给一个String。append是StringBuffer中的方法，不是String的方法

3. hadoop输出key和value中的间隔默认为制表符

   如果要更改，需要设置conf

   ```java
   conf.set("mapred.textoutputformat.ignoreseparator", "true");
   conf.set("mapred.textoutputformat.separator", ", ");
   ```

4. Windows下的hadoop自带bug
   这是我本次作业花费大量时间的地方了，说起来其实就是一句话，hadoop自己在windows下有bug。
   因为在虚拟机下运行实在是太慢了，bdkit毕竟是公共资源能不占用就不占用，我打算在windows下配置Hadoop。但是按照网上的教程，我总是出现如下错误
   ERROR namenode.NameNode: Failed to start namenode.
   查看配置没有任何问题，在中文社区也找不到解决方案，最后在某个网站发现是hadoop3.2.1在windows下自身的问题，需要更换其中的一个文件。
   解决方法在https://kontext.tech/column/hadoop/379/fix-for-hadoop-321-namenode-format-issue-on-windows-10
