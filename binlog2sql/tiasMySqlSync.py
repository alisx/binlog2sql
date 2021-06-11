# 一个程序两种模式，备份模式，和导入模式
# 备份模式，每分钟调用一次导出 sql，将文件存储在同步目录，然后调用同步命令同步文件，完成之后，移动文件到备份目录，结束
# 导入模式，每分钟扫码一次同步目录，将文件安装顺序执行，完成后将文件移动到以处理目录
# 如果出错需要发出警报，并且还需要记录处理日志
# 如果处理出错需要停止，以及需要等待文件的顺序，所以各自需要记录自己的的文件编号指针，一单发现文件不是顺序的立即停止，等等，如果超过10分钟仍然未获取到文件，则报错
import sys, os
from binlog2sql import createSql
import configparser

if __name__ == "__main__":
    configFile = "%s/config.ini" % os.path.split(os.path.realpath(sys.argv[0]))[0]
    if not os.path.exists(configFile):
        print("没找到配置文件，请确认，配置文件路径应该是:", configFile)
        exit(1)
    
    config = configparser.ConfigParser()
    config.read(configFile)
    
    mode = "p" if len(sys.argv) == 1 else sys.argv[1]
    debug = True if len(sys.argv) < 3 else sys.argv[2]=='debug'
    
    ## 如果是生产者
    if mode == "p" or mode == "producer":
        conf = config['Producer']

        if not os.path.exists(conf['sqlFilePath']):
             os.makedirs(conf['sqlFilePath'])

        if not os.path.exists(conf['sqlFileBakPath']):
             os.makedirs(conf['sqlFileBakPath'])

        if createSql(conf):
            # 记录日志
            if debug:
                print("设置配置 fileId:%s\nposision:%s\nbinlogfile:%s" % (conf['fileId'],conf['position'],conf['binlogfile']))
            else:
                print("保存 设置配置 fileId:%s\nposision:%s\nbinlogfile:%s" % (conf['fileId'],conf['position'],conf['binlogfile']))
                with open(configFile, 'w') as c:
                    config.write(c)
            #同步文件
            cmd = "rsync -aPzv %s %s" % (conf['sqlFilePath'], conf['rsyncDest'])
            if debug:
                print("同步文件命令：", cmd)
                result = 0
            else:
                print("执行 同步文件命令：", cmd)
                result = os.system(cmd)
                
            if result == 0:
                print("同步文件完成")
                cmd = "mv -f %s* %s" % (conf['sqlFilePath'], conf['sqlFileBakPath'])
                if debug:
                    print("移除文件命令：", cmd)
                else:
                    print("执行 移除文件命令：", cmd)
                    os.system(cmd)
                    
                print("文件备份完成")
            else:
                print("同步文件失败")
            
    elif mode == "c" or mode == "consumer":
        ## 如果是执行的一段
        ## 读取
        conf = config['Consumer']
        if not os.path.exists(conf['sqlFilePath']):
             os.makedirs(conf['sqlFilePath'])

        if not os.path.exists(conf['sqlFileBakPath']):
             os.makedirs(conf['sqlFileBakPath'])

        files= os.listdir(conf['sqlFilePath']) #得到文件夹下的所有文件名称
        sqls = []
        for file in files: #遍历文件夹
             if not os.path.isdir(file): #判断是否是文件夹，不是文件夹才打开
                sqls.append(file)
                
        if len(sqls) > 0:
            sqls.sort(key=lambda x: int(x.split(".")[-1]))
            logIndex = None ## 应该从上次的记录中获取
            index = conf.getint('fileId') if conf.getint('fileId') else 0 #logIndex if logIndex else (int(sqls[0].split(".")[-1])-1)
            successSql = []
            for sql in sqls:
                sqlid = int(sql.split(".")[-1])
                expectIndex = index + 1
                if expectIndex == sqlid:  # 为了确保连续性
                    # 可以执行文件
                    cmd = "mysql -h%s -u%s -p%s < %s%s" % (conf['host'], conf['user'], conf['password'], conf['sqlFilePath'], sql)
                    if debug:
                        print("数据导入命令:", cmd)
                        result = 0
                    else:
                        print("执行 数据导入命令:", cmd)
                        result = os.system(cmd)
                    if result == 0:
                        successSql.append(sql)
                        index = sqlid
                    else:
                        # 记录错误退出
                        print("执行sql出错")
                        break
                elif expectIndex > sqlid:  # 出现了不连续就停止
                    continue
                else:
                    print("出现不文件id连续 下一个应该为：%d，实际为 %d" % (index+1, sqlid))
                    break
            # 记录日志 
            if len(successSql)>0:
                # 记录日志
                conf['fileId'] = str(index)
                if debug:
                    print("设置配置 fileId:", index)
                else:
                    print("保存 设置配置 fileId:", index)
                    with open(configFile, 'w') as c:
                        config.write(c)
                
                # 移除文件
                for sql in successSql:
                    cmd = "mv -f %s%s %s" % (conf['sqlFilePath'], sql, conf['sqlFileBakPath'])
                    if debug:
                        print("移除已执行文件命令:", cmd)
                    else:
                        print("执行 移除已执行文件命令:", cmd)
                        os.system(cmd)
            else:
                print("没有需要导入的文件")
            
        else:
            print("没有需要处理的文件")
            
