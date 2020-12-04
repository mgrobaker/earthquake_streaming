Reads accelerometer data to detect earthquakes in real time and enable historical analysis.

# Table of contents
1. [Slides](README.md#slides)
2. [Motivation](README.md#motivation)
3. [Demo](README.md#demo)
4. [Tech Stack](README.md#tech-stack)
5. [Code Walkthrough](README.md#processing)
6. [Future work](README.md#setup)

# Slides
I developed this project during my Data Engineering fellowship with Insight Data Science. I presented the results, with an accompanying set of slides.
Links to the deck and recording of my presentation are below:

[Slide deck](https://docs.google.com/presentation/d/1QIKQ1O4YmnweqfO2Gg-yQMwWrTOK3RKLzsc8yyR0pn8/edit#slide=id.p)

[Recorded presentation](https://www.youtube.com/watch?v=f7M_a2ShRWY)

This readme can be considered a complement to the links above. There is some overlap, but the content in the readme is more detailed in some areas.

# Motivation
Earthquakes put thousands of people at risk each year. While we know where they are likely to happen, we do not know when they will strike.

This map shows areas of the world at high risk for earthquakes.

![image](https://user-images.githubusercontent.com/5614366/101183203-2d7c3b80-361d-11eb-818f-42961db6eb6e.png)

## Legacy systems
Many countries in these high risk zones have built early warning systems. These sytems attempt to detect earthquakes as they are starting.

Such systems have limitations. They are expensive and often slow to develop. Further, there is a lot of rework since each country develops its own earthquake detection system on its own. Finally, the systems themselves often have a high false positive rate and therefore are not very effective.

## OpenEEW solution
To address these shortcomings, an organization called OpenEEW (short for "early earthquake warning") has made an open source solution. It detects earthquakes using cheap $50 IoT accelerometers. As of 2020, they have deployed several dozen of these across Latin America, with plans to expand across the globe.

They have made the accelerometer data publically available: there is 1TB of it already on S3. This represents 3 years of sensor data, from 2017 to date.

## Opportunity to build on OpenEEW
I reviewed the documentation, code, and data available from OpenEEW. I determined that one area to improve is in building a data platform on top of their data. This would address two use cases:

1. Scale to enable more advanced real time detection
As OpenEEW grows, I would expect them to add more sensors, which will be sending even more real-time data. Further, they may wish to deploy more advanced earthquake detection models than they are currently running.

2. Study historical earthquake events
OpenEEW is collecting interesting and valuable data. However, at present there is no method for tagging and studying past earthquakes as detected by the system. Studying past earthquakes is likely of interest for analysts and researchers.

A data platform would help enable both of these use cases.

# Demo
This demo shows how some historical earthquakes are captured by my solution. I am visualizing the data in Tableau.

Larger circles indicate earthquakes of higher intensity. Darker circles indicate earthquakes with longer durations. The circles are at sensor locations in Mexico.

[Demo video](https://youtu.be/Sn6wSt9VvnA)

[Demo link](https://public.tableau.com/profile/mark.grobaker#!/vizhome/QuakeWatchDemo/Dashboard)

# Tech stack
I deployed a Pulsar cluster using the "from bare metal" guide, available from Pulsar docs [here](https://pulsar.apache.org/docs/en/deploy-bare-metal/). I hosted the cluster on several AWS EC2 instances.

The cluster contained the Pulsar brokers, bookies, and zookeeper. Two separate machines were used as the Pulsar producer and consumer.

The producer pulled down data from the OpenEEW S3 bucket, and then sent it to the Pulsar cluster. A simple threshold is used to detect high acceleration values. When this threshold is triggered (ie, real time detection), data is sent to Postgres for historical analysis.

Here is a picture of the whole tech stack:
![image](https://user-images.githubusercontent.com/5614366/101184543-da0aed00-361e-11eb-8efc-a668cfe39458.png)

My AWS instances:
![image](https://user-images.githubusercontent.com/5614366/101184635-f3ac3480-361e-11eb-96cc-bebe87b5071b.png)

This shows how Pulsar cluster interacts with clients. Further details are available on the Pulsar website
![image](https://user-images.githubusercontent.com/5614366/101184716-0888c800-361f-11eb-9e41-731582ec174b.png)

# Code Walkthrough
This section walks through the code available in this repo.

## Code run on producer
This code is run on a producer EC2 instance that sends data to Pulsar.

`producer/import_data.py`
This pulls down data from OpenEEW's S3 bucket by using the boto library, and stores it as local files.

As shown in the code, I am only downloading a subset of the data, by limiting to specific dates and locations. I did not need all history for the purposes of this project.

OpenEEW has a library for downloading their data, but it is not functioning properly at present. I opened a ticket to report this bug. I had to implement the data pulling on my own; import_data.py would have been much shorter if the OpenEEW library had been working.

download_data() is the longest function in the file.

`producer/send_data.py`
This sends the data to the Pulsar cluster located at broker1_url. The logic is similar to import_data.py, but with some differences. It loops through the desired time ranges to locate and open the relevant data files. It then steps through each line of the file and sends each line to Pulsar.

send_data() is the longest function in the file. This function also contains some logic to time how quickly messages are being sent to the cluster.

## Code run on consumer
This code is run on a consumer EC2 instance that receives data from Pulsar.
I also installed postgres on this same instance. Postgres could be put on another machine instead, if an alternative architecture is desired.

`consumer/consumer.py`
This listens for messages being sent to the topic. As they are received, it checks if the values should be stored for later analysis. If so, it loads them to the local postgres database.

Let's recall the use case here. In this project, we are playing back history from 9/1/2020 - 9/10/2020. This is to simulate data coming across in real time. Now, most of this data being sent will be noise: slight shakings of the accelerometer from things like a nearby stream or a truck driving by. Actual earthquakes will be more rare, and also will cause much higher readings in the accelerometer. So the interesting data to store will be when an earthquake is going on, not when it is just noise.

I applied a simple threshold to determine if an accelerometer reading is "interesting". My heuristic just says, if the reading is above some level, then let's save it in postgres. (I didn't build an advanced earthquake detection algorithm, since this is not a data science project.)

Returning to the code: there are helper functions to make and insert lists. make_insertion_list is called on every line (second) of data. That line contains ~35 readings each for x, y, and z accelerometer readings. While we could store every one of those readings, for simplicity we aggregate it up to the second level. We take the max and mean of the readings, and then check to see if the mean exceeds the threshold. If it does, then it is "interesting" and we want to store it.

We create a new row for postgres, which has the same number of columns as the table we will insert them in. The function returns a list of these rows.

Finally, back in the listen function, we insert the data into postgres.

`consumer/process_accel_data.py`
This script applies only to the postgres database. It does not connect to the Pulsar cluster. Whereas consumer.py would be running all the time, process_accel_data.py could be run on a daily or weekly basis.

This is an ETL job. It reads the table which the consumer has written to ('accel_data'), transforms it, and loads the result in 'sensor_events' table. (This table is what is used to create the Tableau)

The business purpose here is to group up acceleration readings into what I am calling sensor events. In the case of consecutive high readings, we have an event. For each such event we identify, we can make a key for it, and store when it started and how long it lasted. We can also calculate and store average and peak values.

## What Isn't Here
As mentioned, I deployed a Pulsar cluster on AWS. I do not include the configuration files I used for that cluster. The code in this repo would have to be modified with the connection details of the particular Pulsar cluster you are connecting to.

# Future work
Given more time, here are some interesting areas to work on in this project.

## Improve Pulsar usage
### Topic architecture
Build a more sophisticated topic architecture. Right now, all data is sent to the 'sensors' topic. However, a DAG could be designed to direct messages from this topic to others. For example, we may want subtopics by country, or by intensity of detected acceleration. That is, you might have 'all-sensors' --> 'mexico-sensors' --> 'mexico-quakes'.

### Functions
I spent a while trying to deploy functions in Pulsar, and was able to figure most of it out. However I have left the detection function in the consumer for now as it is simpler.
A followup to this project might be to deploy that detection function within the cluster itself.

### Tiered storage
One interesting feature that originally attracted me to Pulsar is its ability for tiered storage. This allows you to store past messages in cheap storage such as S3, which you can then query with PulsarSQL. I didn't get time to implement these features of Pulsar but they would be interesting to explore.

## Improve throughput
I did some testing around message throughput and tried a few optimizations. However I was not able to increase it significantly. Further work could be done here.

I tried topic partitioning, which enables all the machines in the Pulsar cluster to read from the topic, and not just one. I also tried bulk acknowledgement, which acks many messages at once instead of one by one.

For now, I am doing an async_send, which sends messages as fast as possible without waiting for an acknowledgement.

In my consumer I am also writing to Postgres. I did confirm this is not slowing down the listening to the Pulsar messages.

## Read real-time OpenEEW data
You could connect to OpenEEW's real time messaging service and process those messages in Pulsar.
In this project I played back some of history to simulate the data coming across in real time.

## Improve earthquake detection functions
Optimizing the detection is more of a data science undertaking. However OpenEEW already does have some pretty good detection functions in their [repo](https://github.com/openeew/openeew). This project could use those detection functions.

## Load government earthquake data to postgres
Many national governments store data about earthquakes. You could pull down this data, and land it in postgres. From there you could compare government-reported quakes with OpenEEW-detected quakes to see what the differences are.
