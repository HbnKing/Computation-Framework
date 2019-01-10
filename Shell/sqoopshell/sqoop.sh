 #!/bin/bash
 #这个脚本是通过循环读取HDFS上面的文件，然后执行对应的sqoop脚本实现对数据的导入操作
 #这是将oracle数据的数据导入到hive当中
 main=main.sh   #这个是主脚本，在主脚本当中调用真正执行的shell脚本
 logfile=log.txt
 date=`date +%Y%m%d`
#遍历HDFS上面文件夹当中的文件。awk默认是按照空格进行分割的，然后拿出这个目录下的文件
 hadoop fs -ls /user/gxg/data/shell/* | awk '{print $8}' > files0.txt
 # 查看文件，然后按行去读取这个文件中行信息
 cat  files0.txt | while read line
 do
 #这里是将这个路径信息按照/的方式分割，然后拿到最后的文件/user/gxg/data/shell/main.sh
 #注意我这是第六个元素。自己的可以按照自己的想法获取
 _path=`echo $line | awk -F'/' '{print $6}'`
 echo "$_path"
 #这里是过滤掉那个主脚本，因为他不是正真到数据的脚本，放在里面对脚本执行有影响
 if [ "$_path" = "$main" ]
   then
      echo "脚本为main，跳过执行主脚本"
      #这里如果是主脚本就跳过执行
      continue
 fi
 #这里是执行正真的sqoop导入脚本。
 source ./$_path
echo "$_path"
#下面是对上一条执行命令进行异常的捕获。然后将数据执行结果输入到对应的日志文件当中
#这里需要注意的是，我们将日志输入到对应的文件下面去。但是我们是采用oozie调度的方式实现的，所以在HDFS找不到打印的日志
#最后只能通过cd的命令实现将数据日志写入到对饮的日志文件当中。
 if [ $? -ne 0 ]; then
    echo "这条命令执行失败"
    cd /home/log/
    echo $_path" 执行失败 "$date >> $logfile
else
    echo "这条命令执行成功"
    cd /home/log/
    echo $_path"执行成功"$date >> $logfile
fi

 done