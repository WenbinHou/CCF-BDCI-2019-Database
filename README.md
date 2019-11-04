# CCF BDCI 2019 Database Contest

See [https://www.datafountain.cn/competitions/347](https://www.datafountain.cn/competitions/347) for details.

### System Configurations

**# Allow unlimited memlock, RT scheduling**

In `/etc/security/limits.conf`

````text
<user> soft memlock unlimited
<user> hard memlock unlimited

<user> soft rtprio 99
<user> hard rtprio 99
````

