# �w�˳�����B�J
## �U��Spark 2.1.1
- wget https://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-hadoop2.7.tgz

## �����Y�ɮ�
- tar -zxvf spark-2.1.1-bin-hadoop2.7.tgz

## �N�ɮש�m��/usr/local/spark ��
- sudo mv spark-2.1.1-bin-hadoop2.7 /usr/local/spark

## �]�w�����ܼ�
- sudo vi /etc/profile
```
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
```

## �������ܼƥͮ�
- source /etc/profile


# �w��Cluster�B�J

## �]�w spark-env
- cd /usr/local/spark/conf
- cp spark-env.sh.template spark-env.sh
- cp slaves.template slaves

## �ק� slaves �� spark-env
- vi slaves
```
data1
data2
data3
```

- vi spark-env.sh
```
export SPARK_MASTER_IP=master
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=800m
export SPARK_WORKER_INSTANCES=2
```



## �ק�/etc/ssh/sshd_config
- sudo vi /etc/ssh/sshd_config
```
PasswordAuthentication no
PermitEmptyPasswords yes
```

## ���v���ק�ͮ�
- sudo service sshd restart

## �]�w�L���_�n�J
- ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
- cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
- chmod 700 ~/.ssh
- chmod 600 ~/.ssh/authorized_keys

## �ˬd�O�_��n�J
- ssh localhost

## �NMaster ��Copy

## �ˬd IP
- ifconfig 

## �ä[�ͮ�
- vi /etc/hostname
- ��ڥD��Hostname ���J�W��
```
master
```

## �ק�Hostname (�ߧY���Ȯɥͮ�)
- �bmaster �U: sudo hostname master 
- �bdata1  �U: sudo hostname data1
- �bdata2  �U: sudo hostname data2 
- �bdata3  �U: sudo hostname data3

## �s�� hosts
- vi /etc/hosts
```
    192.168.233.155 master
	192.168.233.156 data1
	192.168.233.157 data2
	192.168.233.158 data3
```

## ����������
- sudo chkconfig iptables off
- sudo service iptables stop

## �ҰʩҦ�Cluster
- /usr/local/spark/sbin/start-all.sh

## �����Ҧ�Cluster
- /usr/local/spark/sbin/stop-all.sh

## �˵��O�_�ҥ�
- master:8080

