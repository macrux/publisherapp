# publisherapp

This is an eclipse project created to publish messages to an OpenMAMA topic using ZQM Bridge.
Please note that parameters to connect to OpenMAMA are harcoded since this program was
created just to replicate an Issue when stopping the publisher.

It will run by default with:

* tport      = pub
* middleware = zmq
* topic      = TEST

If you want to receive the messages you publish, please configure a mama subscriber app with
those parameters (or change parameters in main class to match your prefrences)

Logging libraries are provided in the `/libs` folder, but note that you have to add the
external jar mamajni.jar located in your OpenMAMA installation folder. This project was tested
using OpenMAMA 2.4.0 release.

(You likely have to tweak the eclipse project to make it work with your environment)

To recreate the issue please follow next steps:

1. Start the program. You can generate the jar from file > export > Runnable jar file and then
   execute it with `java -jar <jar-name>.jar` or running the main class from eclipse
2. Press `i` to start the publisher. The publisher will run in a thread  different from main thread
3. if you want to publish some messages press `p`
4. Press `f` to stop the publisher.
5. The issue shows up.
