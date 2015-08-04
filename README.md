# Smart Thermostat - AWS - HBase - Spark

This repository is accompanying the blog post [Making your own smart 'machine learning' thermostat using Arduino, AWS, HBase, Spark, Raspberry PI and XBee](http://niektemme.com/2015/07/31/smart-thermostat/ @@to do). This blog post describes building and programming your own smart thermostat. 

This smart thermostat is based on three feedback loops. 
- **I. The first loop is based on an Arduino directly controlling the boiler.  (this repostiory)**
- II. The second feedback loop is a Raspberry PI that receives temperature data en boiler status information from the Arduino and sends instructions to the Arduino. [Smart Thermostat - Raspberry PI Repository](https://github.com/niektemme/smarttherm-rpi)
- II. The third and last feedback loop is a server in the Cloud. This server uses machine learning to optimize the boiler control model that is running on the Raspberry PI. [Smart Thermostat - AWS - HBase - Spark Repository Repository](https://github.com/niektemme/smarttherm-aws-hbase-spark)

![Smart thermostat overview - three feedback loops](https://niektemme.files.wordpress.com/2015/07/schema_loop3.png)

## Installation & Setup

### Hardware setup
The hardware setup is described in detail in the [blog post]( http://niektemme.com/2015/07/31/smart-thermostat/ @@) mentioned above. 

### Dependencies
The following Arduino libraries are required
- LiquidCrystal (LCD) - should be installed by default
- SoftwareSerial - should be installed by default
- XBee - [Google code page](https://code.google.com/p/xbee-arduino/)

### Installation
The Arduino sketch in the smarttherm subfolder of this repository can be uploaded to the arduino as with any Arduino sketch.

## Acknowledgements
The code used in this project is often based on wonderful and clear written examples written by other people. I would especially like to thank the following people (alphabetical order).

- Aravindu Sandela - bigdatahandler - http://bigdatahandler.com
- Dave - Desert Home - http://www.desert-home.com
- Lady Ada - Adafruit - http://www.adafruit.com
- Lars George - HBase definitive guide - http://www.larsgeorge.com
- Luckily Seraph Chutium - ABC Networks Blog - http://www.abcn.net
- Michael Bouvy - http://michael.bouvy.net
- Paco Nathan - O'Reilly Media - http://iber118.com/pxn/
- Robert Faludi - Digi International - http://www.faludi.com
- Stephen Phillips - The University of Southampton, IT Innovation Centre  - http://blog.scphillips.com
