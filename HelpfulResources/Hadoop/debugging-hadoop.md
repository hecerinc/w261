
## What is my python version?
The jupyter notebook runs an anaconda environment, which is 3.6. If you look at the repo for the environment, particularly this file: https://github.com/UCB-w261/w261-environment/blob/master/start-notebook-python.sh

The system python is what Red Hat has installed on the operating system. This python version is 2.6.6 usually.

This won't matter once we start using mrjob since it'll use the python version of the driver which will be the one the jupyter notebook is based on. However, for the homeworks using Hadoop Streaming you will either need to write mappers and reducers that properly in python 2.6 or use the shebang `#!/opt/anaconda/bin/python` so that they reference the python executable from the anaconda environment.

## Debugging in hadoop streaming
While hadoop streaming is an excellent way to start learning about the nuts and bolts of map-reduce style parallelization, it can be a huge pain to debug. The best advice we can give you is to __test early and test often__. In particular, Python errors in your mapper and reducer can lead to hadoop streaming errors that actually have nothing to do with hadoop. Some tips to avoid this kind of headache:
* Always unit test your python scripts on their own before you run them in hadoop streaming. For example, pipe in some fake data via unix commands (as we do in the example provided in HW2).
* Maintain a working hadoop streaming command as you develop your mapper & reducer code so that you can pinpoint problems when they arise.    
  * One way to do this is to start with a dummy mapper and reducer that simply passes your data through. For example, try using the unix program `/bin/cat` instead of `mapper.py` and/or `reducer.py` in your hadoop streaming command.
  * Once you know your hadoop command is set up correctly (i.e. it runs fine with the dummy program) you can switch out the mapper and reducer for your actual python scripts to test them independently of each other. (Note you should be using some small fake data for this testing and if you want to test the reducer on its own you will need to use fake data that simulates your mapper output).
  * Once the pieces work independently, put it all together. If errors arise it is likely due to an inconsistency in the key-value structure, sorting and/or partitioning strategy you are using to design your solution. Thinking through these problems is the important learning from this part of the course.

Finally, some quick things to check:
* **Are you using the right shebang?** _See python versio notes above._
* **If you are on docker, have you allocated at least 4GB of memory?**
* **Do you have the right jar file path.?** This path will depend on your computing environment. If you are working on the course Docker container the correct jar file is: `/usr/lib/hadoop-mapreduce/hadoop-streaming.jar`. If you are working on your own environment use the unix commands `find` and `grep` to see what `.jar` files are available and select the appropriate one.
* **Did you accidentally put spaces between the arguments you are passing to `-files ` in your hadoop command?** _Don't!_
* **Did you forget to pass an auxillary file that is needed by your mapper or reducer?** _Note: you can pass in directories as well as files._
* **Are the components of your hadoop command in the wrong order?** _Refer to provided examples like the tutorials and notebooks in the Resources folder of this Repo._
* **Have you looked at the Web UI to track your hadoop's MR job progress?"** _The streaming job output will include a url to this UI. What do the logs tell you about when the job fails?_
* **Have you googled the error message?** _There are helpful people on the interwebs... sometimes anyway._

## Log files
You can find more specific erorrs by going to the link in the hadoop streaming output, and looking at the logs for the job:

![Error Logs](error-log-location.png?raw=true)

The error logs are in:   
http://quickstart.cloudera:19888/logs/userlogs/application_1495397902250_0021/

Click on each container, and you will find the log files for the job you are debugging.
