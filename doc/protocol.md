
# mysql协议
## 概述

    报文分为消息头和消息体两部分，其中消息头占用固定的4个字节。
    消息长度用来解决粘包与半包问题问题。
    第四个字节为了防止串包用。机制是每收到一个报文都在其sequenceId上加1，并随着需要返回的信息返回回去。如果DB检测到sequenceId连续，则表明没有串包。如果不连续，则串包，DB会直接丢弃这个连接。
    消息体则是最终传递信息的地方。

报文结构如下：  
![img](1.png)  
## 登录
过程如下：  
![img](2.png)

    Step1:客户端向DB发起TCP握手。
    Step2:三次握手成功。与通常流程不同的是，由DB发送HandShake信息。这个Packet里面包含了加密seed信息。
    Step3:客户端根据HandShake包里面的加密seed对MySql登录密码进行摘要后，构造Auth认证包发送给DB。
    Step4:DB接收到客户端发过来的Auth包后会对密码摘要进行比对，从而确认是否能够登录。如果能，则发送Okay包返回。
    Step5:客户端与DB的连接至此完毕。
## 登录抓包分析
![img](3.png)  

    353，354，355三个报文是客户端与服务端建立tcp连接。
    358是服务端发送的HandShake包。
    360是客户端发送的auth包。
    362是服务端发送的auth ok包。
### HandShake包内容如下
    3b 00 00 00 //数据长度，3字节
    0a //序号，1字节
    35 2e 31 2e 31 2d 66 72 65 65 64 6f 6d 00 //版本信息，字符串，以\0结尾。内容为5.1.1-freedom
    03 00 00 00 //连接ID，4字节
    78 68 43 76 73 74 52 54 //随机加密串的前半部分，定长8字节。
    00 //固定填充0
    4f b7 //服务端属性的低16位，2字节
    21 //字符集，1字节。utf-8。
    02 00 //服务端状态，2字节
    00 00 //服务端属性的高16位，2字节
    00 //固定填充0
    00 00 00 00 00 00 00 00 00 00 //固定填充0，10字节
    69 4a 46 6e 39 52 4b 46 67 71 58 76 00 //随机加密串的后半部分，以\0结尾，加密串总共8+12=20字节
### authentication包内容如下
    39 00 00 //数据长度，3字节
    01 //序号，1字节，同一个动作的所有请求与响应会递增此值
    85 a6 0f 00 //客户端支持的属性，4字节
    00 00 00 01 //最大数据包长度，4字节
    1c //字符集，1字节。0x1c=28=gbk
    00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 //固定填充0
    70 61 79 00 //用户名，\0结尾的字符串，内容为pay
    14 //密码串长度，1字节，0x14=20字节
    1d ea 47 5b 09 03 13 cf cb bf 6c d3 c3 08 03 bc b9 23 af 81  //密码的加密串，20字节

加密算法如下：  
    SHA1(password) XOR SHA1("20-bytes random data from server" <concat> SHA1(SHA1(password)))

具体实现为

    public static final byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
          MessageDigest md = MessageDigest.getInstance("SHA-1");
          byte[] pass1 = md.digest(pass);
          md.reset();
          byte[] pass2 = md.digest(pass1);
          md.reset();
          md.update(seed);
          byte[] pass3 = md.digest(pass2);
          for (int i = 0; i < pass3.length; i++) {
              pass3[i] = (byte) (pass3[i] ^ pass1[i]);
          }
          return pass3;
      }


### auth ok
    07 00 00 //数据长度，3字节  
    02 //序号，1字节，在上一个包的基础上又+1了
    00  //状态标识，1字节，0x00表示成功
    00 //影响行数，变长数值
    00 //LastInsertId，变长数值
    02 00 //状态，2字节
    00 00 //消息
## 参考
https://my.oschina.net/alchemystar/blog/833598  
http://hutaow.com/blog/2013/11/06/mysql-protocol-analysis/  
https://www.cnblogs.com/niuyourou/p/12538546.html  
https://developer.aliyun.com/article/432862  
