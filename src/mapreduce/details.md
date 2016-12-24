####Lab1共有五个部分,需要阅读MapReduce的[原始论文](/mapreduce.pdf)。  
论文不是很难理解，mapreduce原理很简单，感觉类似java 7的fork/join框架。我想要实现这样一个可以用于生产环境的系统更多是关于工程方面的问题。在这里想感慨一下，前几日参加阿里的宣讲会，一学长给我介绍了一下阿里的四代架构，貌似是04的第一代架构还是很原始的形式。那时候google已经做出了map/reduce的第一代实现了。不过今年双十一，阿里能处理的并发量已经达到12w笔交易/s了。经过十几年，中国的技术已经有了很大的进步了。
####Part1 Map/Reduce input and output  
该部分需要我们完成doReduce和doMap这两个函数。mapreduce论文图一给出了map/reduce的基本工作流程。这一部分需要我们完成读取分割之后的文件里的内容，然后调用用户写好的map函数处理读取到的文件内容，并以{key,value}对储存在中间文件中，中间文件的文件名须调用reduceName（）生成。关于读取输入文件以及生成中间文件，论文里提到了两点： 
* 输入文件是由GFS管理，储存在集群服务器的本地磁盘里。而且GFS会将每个文件分割为64M大小的块，并将该块拷贝3份分别储存在不同的机器上。因此在调度分配Task时，Master会将Task分配给储存有该Task相关的文件块的Worker，已减少网络中需要传输的数据量。这点很重要，有文章指出运行在数据中心之上的分布式应用会由于底层网络资源的限制，而降低20-30%的性能。  
* map阶段生成的中间文件存储在本地磁盘，Master只要把中间文件的文件名告诉执行reduce任务的Worker即可。对于每个Map worker得到的临时key/value对，储存在#nReduce个中间文件里。对key值算hash取模决定放置哪个中间文件里。每个reduce worker需要读取各个map worker生成的中间文件对应着自己的那一个。

因此对于doMap，读取inFile文件的内容，调用map方法，并对生成的keyvalue数组中每个key值计算hash值，取模。储存在对应的文件中。[具体实现](./common_map.go)<br>对于doRuduce,从Task对应的临时中间文件里读取key/value对，维护一个map[string][]string的数据结构，储存相同key值的values。然后将该数据结构里每条键值对传递给用户指定的reduce函数。将返回值作为该key值的value值，储存在输出文件里。[具体实现](./common_reduce.go)  
####Part II Sigle-worker word count  
该部分需要我们完成reduce函数和map函数，统计文本里单词出现的次数。对于map函数，分割传入的字符串：  
```
words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
```

对于每个word构建一个KeyValue{word,"1"},并维护一个储存KeyValue的slice。  
对于reduce函数，因为对于每一次word的出现，我们便构建了一个KeyValue，而之前实现的doReduce会将相同key值的value储存在一个slice里传递给reduce函数，因此这里我们只slice的长度就是key出现的次数。这里要注意下，len(slice)返回的int类型，但我们写的reduce需要返回string类型。需要使用strconv.Itoa()将int转换为string类型。[具体实现](../main/wc.go)
####Part III:Distributing MapReduce tasks  and Part IV:Handling worker failures
在这一部分，我们需要实现master调度任务的程序。Mit 6.824实现map/reduce的基本工作流程是：
* 用户指定map，reduce函数，以及nReduce表示执行reduce的worker的数量。因为，根据输入文件分割之后的文件数目，不需要指定执行map的worker的数目。调用master.go中的run函数  
* run中，会以此调用schedule(mapPhase),schedule(reducePhase)执行map和reduce阶段  
* shcedule()会使用rpc调用注册的worker的doTask函数，执行任务  
* 这两个阶段完成之后，调用merge合并reduce生成的文件，作为最后的输出结果

master会维护一个slice workers记录注册的worker，和一个channel ，传递注册的worker,registerChannel。worker注册时，master会首选将该worker添加进自己维护的workers数据结构中，然后将该worker传递进registerChannel。
```
registerChannel chan string
workers  []string
```

无论是在map还是reduce阶段，新建goroutine处理每个需要worker执行的Task。通过mr.registerChannel接受注册的worker，通过rpc调用该worker的DoTask。这里我们使用该课程自己实现的rpc框架。该框架封装了go net/rpc库。对于执行失败的Task，继续取mr.registerChannel传递进来的worker执行。若执行成功，将该worker传递进mr.registerChannel，以执行其他任务。
```
go func(taskNum int,nios int, phase jobPhase) {
	for {
		worker := <- mr.registerChannel
		ok := call(worker,"Worker.DoTask",&taskArgs,new(struct{}))
		if ok {
			//success, break and put the worker into channel
			go func(){
				mr.registerChannel <- worker
			}()
			break
		}
		//failure,just retry
	}
	}(i,nios,phase)
```

我们需要等待所有的Task都执行完之后，才能退出schedle函数，不然可能导致下一阶段出现失败。可以使用go提供的sync.WaitGroup。  
WaitGroup用来等待一组协程完成工作。有三个方法：  
* Add(int) 增加WaitGroup内部的计数器的值
* Done()  表示一个协程已经完成工作了，将WaitGroup计数器的值减1，其实就是调用Add(-1)
* Wait() 同步调用，因此会被阻塞直到WaitGroup内部计数器的值为0方法返回

[具体实现](./schedule.go)

####Part V：Inverted index generation  
这部分需要我们实现倒排索引，即统计每个单词出现在哪些文本里，同时记录这些文本。和第二部分的word count类似，不过需要主要最后的输出格式：
```
word: #documents documents,sorted,and,separated,by,commas
可以使用fmt.Sprint（）生成指定格式的字符串
```

[具体实现](../main/ii.go)
