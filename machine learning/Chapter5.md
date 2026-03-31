# Chapter 5: Retrieving Data from APIs

## Learning Objectives

- Students will be able to make HTTP GET requests to REST APIs using the Python requests library and interpret HTTP status codes
- Students will be able to parse nested JSON responses and extract relevant data fields using dictionary and list indexing
- Students will be able to construct API requests with querystring parameters to filter, search, and paginate results
- Students will be able to design and implement pagination loops to retrieve complete datasets from APIs across multiple requests
- Students will be able to convert API responses into Pandas DataFrames for downstream analysis

---

## 5.1 Introduction

In earlier chapters, you learned how to collect data by creating it yourself or by extracting it from websites using web scraping techniques. While web scraping can be useful, it is often fragile, unstructured, and dependent on the layout of a website that you do not control. In professional analytics workflows, a more reliable and scalable approach is preferred whenever possible.

One of the most common and powerful ways analysts retrieve data today is through **Application programming interfaces (APIs)** — A structured interface that allows one software system to request and receive data from another system in a standardized way.. APIs are intentionally designed to provide clean, documented, and repeatable access to data. Instead of manually downloading files or scraping web pages, analysts can use APIs to automate data collection and integrate external data directly into their analysis pipelines.

Many modern APIs are delivered as **RESTful web services** — A web-based API style that uses standard HTTP requests and URLs to access structured data resources.. RESTful APIs typically allow users to retrieve data using simple web requests and receive results in structured formats such as JSON. In this chapter, you will primarily work with requests that retrieve data rather than modify it, focusing on how to access and interpret the information returned by an API.

From an analytics perspective, APIs serve as modular data services. Much like building with LEGO bricks, analysts can combine multiple APIs to enrich datasets, update information automatically, or swap out one data source for another with minimal changes to their code. This modular approach supports flexible, distributed data workflows that are common in modern organizations.

A typical API-based workflow involves requesting data from an API endpoint, receiving a response in JSON format, converting that JSON into a Pandas DataFrame, and then applying the same data wrangling, analysis, and modeling techniques you have already learned. Because APIs are designed for programmatic access, they are generally more stable, ethical, and efficient than scraping data from websites that were not intended for automated use.

Throughout this chapter, you will work with several real-world examples of APIs, including public data services, APIs that require authentication keys, and APIs commonly used in business and analytics contexts such as health data, financial markets, and consumer platforms. By the end of the chapter, you will be able to retrieve data from web services and integrate it seamlessly into your analytics workflows.

---

## 5.2 Web Service APIs

### How Do APIs Work?

#### Step 1: The Request

An API interaction begins when a client application sends a request to a specific API **endpoint** — A URL that represents a specific resource or data service provided by an API.. For analysts, this client is often a Python script or notebook rather than a web or mobile application.

Most analytics-focused API requests use the HTTP GET method to retrieve data. A request always includes the endpoint URL and may also include parameters, headers, or authentication credentials depending on the API.

- **URL**: The address of the API endpoint being requested.
- **Parameters**: Key–value pairs added to the URL to filter, search, or limit the returned data.
- **Headers**: Metadata sent with the request that may specify authentication, data format, or versioning.
- **Payload**: Data sent to the API for processing, typically used with POST or PUT requests rather than data retrieval.

In this chapter, you will primarily work with GET requests that retrieve data rather than modify it.

#### Step 2: Processing the Request

After receiving the request, the API processes it by validating inputs, checking permissions, and retrieving or computing the requested data. This may involve querying databases, calling internal services, or applying business logic before a response is generated.

From the client’s perspective, this processing is invisible. The client simply waits for a response.

#### Step 3: The Response

Once processing is complete, the API returns a response that includes a status code indicating success or failure, optional headers, and the requested data formatted as JSON or XML. JSON has become the dominant format because it is compact, readable, and easy to work with in Python.

APIs are widely used because they allow organizations to centralize data and logic while supporting many different applications and users. Compared to web scraping, APIs are typically more stable, efficient, and ethically appropriate for automated data access.

### API Methods

APIs support several HTTP request methods. While analytics work typically relies on GET requests, it is helpful to understand the broader set of methods.

### Data Formats

APIs typically exchange data using structured, self-describing formats. The two most common formats are JSON and XML, both of which can represent nested, hierarchical data.

JSON is now the most widely used format for APIs because it is lightweight, readable, and maps naturally to Python dictionaries and lists. XML is still used in some legacy systems but is less common in modern analytics workflows.

#### JSON Example

JSON, like most languages, ignores white space. Therefore, it can be written on a single line or nested on multiple lines as in the two examples below:

```python
{"employees":[{"firstName":"Suzy","lastName":"Smith"},{"firstName":"Bob","lastName":"Jones"},{"firstName":"Abigail","lastName":"Adams"}]}

{
  "employees": [
    {
      "firstName": "Suzy",
      "lastName": "Smith"
    },
    {
      "firstName": "Bob",
      "lastName": "Jones"
    },
    {
      "firstName": "Abigail",
      "lastName": "Adams"
    }
  ]
}
```

#### XML Example

```python
<employees><employee><firstName>Suzy</firstName><lastName>Smith</lastName></employee><employee><firstName>Bob</firstName><lastName>Jones</lastName></employee></employees>

<employees>
  <employee>
    <firstName>Suzy</firstName>
    <lastName>Smith</lastName>
  </employee>
  <employee>
    <firstName>Bob</firstName>
    <lastName>Jones</lastName>
  </employee>
  <employee>
    <firstName>Abigail</firstName>
    <lastName>Adams</lastName>
  </employee>
</employees>
```

### Authentication Methods

Many APIs require authentication to control access, monitor usage, and prevent abuse. Authentication determines who is allowed to make requests and how frequently.

- **No Authentication**: Open APIs that allow unrestricted access, which are increasingly rare.
- **Basic Authentication**: Uses a username and password; simple but generally discouraged due to security risks.
- **API Keys**: A unique identifier passed with requests that allows providers to track usage and enforce rate limits.
- **OAuth**: A token-based system used when accessing user-specific or sensitive data.

### Public APIs

Many APIs are publicly accessible and provide high-quality data suitable for analysis. These APIs often require registration (and sometimes an API key) but do not require payment for limited usage. In this section, you will explore a curated list of options and then review additional APIs that are especially useful for analytics projects.

The Public APIs GitHub repository maintains a curated list of free APIs across many domains. You can browse it here: https://github.com/public-apis/public-apis. [oai_citation:0‡GitHub](https://github.com/public-apis/public-apis)

Table 1 includes (1) 10 interesting options drawn from the GitHub Public APIs collection, plus (2) additional APIs for sports (excluding ESPN), games, books, finance, and general analytics datasets. Always review each API’s usage notes, rate limits, and authentication requirements before building a large data pull.

---

## 5.3 In Python

### Basic APIs

Python has several libraries for calling web APIs to retrieve data. In this chapter, we will use the _requests_ package, which you previously used for web scraping.

We will begin with free, basic APIs that do not require authentication, headers, parameters, or request body data. These APIs are ideal for learning because they allow you to focus on the mechanics of making requests and interpreting responses without additional setup.

Because open APIs can change or go offline without notice, it is always good practice to check the status code and inspect the response before assuming the data is valid. Later in this chapter, you will learn how to handle failures more robustly.

- https://randomuser.me/api/ — Returns demographic and profile information for a randomly generated user.
- https://dog.ceo/api/breeds/image/random — Returns a random dog image URL.
- https://official-joke-api.appspot.com/random_joke — Returns a random programming joke.
- https://api.zippopotam.us/us/33162 — Returns geographic information for a U.S. ZIP code.
- https://ipinfo.io/161.185.160.93/geo — Returns geographic metadata for an IP address.
- https://isro.vercel.app/api/spacecrafts — Returns a list of spacecraft launched by ISRO.
- https://catfact.ninja/fact — Returns a random cat fact (simple JSON key “fact”).
- https://www.boredapi.com/api/activity — Suggests a random activity (good for practice with non-nested JSON).
- https://api.ipify.org?format=json — Returns the client’s public IP address as JSON.
- https://www.themealdb.com/api/json/v1/1/random.php — Returns a random meal recipe with structured JSON.

The example below demonstrates how to make a simple GET request using the Random User API.

```python
import requests

# '.get' refers to the request method being used (GET, POST, PUT, DELETE, etc.)
response = requests.get("https://randomuser.me/api/")
print(response.status_code)

# Output:
# 200
```

A status code of **200** indicates that the request was successful and the server returned data. The table below summarizes some common status codes, what they mean, and likely causes.

To see the raw data returned by the API, access the _.text_ attribute of the response object.

```python
response.text

# Output:
# {"results":[{"gender":"female","name":{"title":"Miss","first":"Gretl","last":"Wiegmann"},"location":{"street":{"number":6445,"name":"Neue Straße"},"city":"Gedern","state":"Thüringen","country":"Germany","postcode":82979,"coordinates":{"latitude":"-49.5994","longitude":"111.6012"},"timezone":{"offset":"-9:00","description":"Alaska"}},"email":"gretl.wiegmann@example.com","login":{"uuid":"5f11ab4f-c868-42d0-8636-3cf1fb61dfb5","username":"happyelephant814","password":"chicago1","salt":"FyGj1dsy","md5":"e446e6c2997ef9684a409d6f31166011","sha1":"069d9e06c02c3e65f8eb5669d44166c9e7e9f0b7","sha256":"ab7c3f62396d4499a983e1ca16931b0d1d2379e0e71a8dd051c49f661f877b9c"},"dob":{"date":"1998-07-06T22:52:25.360Z","age":27},"registered":{"date":"2003-09-08T07:57:57.166Z","age":22},"phone":"0740-8973298","cell":"0170-8616661","id":{"name":"SVNR","value":"73 060798 W 744"},"picture":{"large":"https://randomuser.me/api/portraits/women/96.jpg","medium":"https://randomuser.me/api/portraits/med/women/96.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/96.jpg"},"nat":"DE"}],"info":{"seed":"fea5b9de92752393","results":1,"page":1,"version":"1.4"}}
```

The returned data is formatted as JSON, which is a structured, text-based representation of nested dictionaries and lists. Python provides tools to convert JSON strings into native data structures.

```python
import json

json_data = json.loads(response.text)
print(json.dumps(json_data, indent=2))
```

Okay, that helps. JSON is basically a complex Python dictionary of nested lists and dictionaries in the form of key/value pairs. For example, the first label "results" is a key, while the list that appears on the other side of the ":" symbol is the value for that key even though that list contains one and only one item: a very large dictionary containing information about the generated person.

Our goal is to extract the data from this JSON object that is relevant to us. In this case, we want to convert a few fields into a small table so that we can store it and export it into a .CSV file. Let’s first review how to drill down through a JSON object.

```python
# Start with the entire json_data object
# Then go into the value of the 'results' key
# Then go to the first item in the list (index 0), which is a dictionary
# Then go to the 'location' key of that dictionary and return the value (another dictionary)
json_data['results'][0]['location']

# Output:
# {'street': {'number': 6445, 'name': 'Neue Straße'},
#  'city': 'Gedern',
#  'state': 'Thüringen',
#  'country': 'Germany',
#  'postcode': 82979,
#  'coordinates': {'latitude': '-49.5994', 'longitude': '111.6012'},
#  'timezone': {'offset': '-9:00', 'description': 'Alaska'}}
```

Do you see how each of these parts—['results'], [0], and ['location']—drill down further into the JSON object? This is important to understand, so ask your instructor for help if you are still not sure. Remember that lists are accessed with positions (e.g., 0), whereas dictionaries are accessed by key names (e.g., 'results' and 'location'). Both use the square bracket notation []. This is different from how lists and dictionaries are created ([] for lists, {} for dictionaries).

Now that we know how to drill down to only the feature values we want, let’s put it all together into a Pandas DataFrame by creating a loop that calls the API multiple times and adds one row per user.

```python
import pandas as pd
import json
import requests

df = pd.DataFrame(columns=['name', 'gender', 'age', 'city', 'state'])
df.set_index('name', inplace=True)

# Generate several users
for i in range(5):
  response = requests.get("https://randomuser.me/api/")
  json_data = json.loads(response.text)

  name = json_data['results'][0]['name']['first'] + " " + json_data['results'][0]['name']['last']
  df.loc[name] = [json_data['results'][0]['gender'],
                  json_data['results'][0]['dob']['age'],
                  json_data['results'][0]['location']['city'],
                  json_data['results'][0]['location']['state']]

# Save the results to a .CSV file and print the DataFrame
df.to_csv('users.csv')
df.head()

# Output:
# 	            gender	age	    city	    state
# name
# Hudson Park	male	54	Radisson	British Columbia
# Gonzalo Bravo	male	68	Móstoles	Andalucía
# Nicolas Ortega	male	32	Santander	Ceuta
# Matias Huotari	male	41	Varkaus	Southern Savonia
# Afet Özbey	female	49	Yozgat	Karabük
```

Remember that you will have different data in this table than the example because the API returns randomly generated users. Now you have a dataset that you can work with. It only has a few rows, but the code is dynamic and will work whether there are 3 or 3000+ rows.

### Endpoints

You may have noticed a term used above: _endpoint_. The type of APIs we are using here is a specific type referred to as _REST web services_. Web services have a URL (e.g., https://www.domainname.com) and one or more endpoints (e.g., https://www.domainname.com/api/**endpointname**). An **endpoint** — The location from which each of the functions offered by REST Web Service APIs can be accessed. is a specific functionality offered by a RESTful web service. Each endpoint has its own name, which is simply attached to the web service URL, thus giving each endpoint its own location.

Every web service API will have its own documentation that details each available endpoint, the method for calling each endpoint (e.g., GET or POST), inputs required, and outputs delivered. You will see these details in action through the rest of this topic. Let’s try another web service from the earlier list: https://isro.vercel.app/api. This API has four endpoints that provide data about spacecraft, launchers, customer satellites, and ISRO centres. Let’s use each endpoint below.

```python
import requests

# Returns a list of ISRO launched spacecraft
response = requests.get("https://isro.vercel.app/api/spacecrafts")
print(response.json())

# Returns a list of ISRO launchers
response = requests.get("https://isro.vercel.app/api/launchers")
print(response.json())

# Returns a list of ISRO customer satellites
response = requests.get("https://isro.vercel.app/api/customer_satellites")
print(response.json())

# Returns a list of ISRO centres
response = requests.get("https://isro.vercel.app/api/centres")
print(response.json())

# Output:
# {'spacecrafts': [{'id': 1, 'name': 'Aryabhata'}, {'id': 2, 'name': 'Bhaskara-I'}, {'id': 3, 'name': 'Rohini Technology Payload (RTP)'} ...
# {'launchers': [{'id': 'SLV-3E1'}, {'id': 'SLV-3E2'}, {'id': 'SLV-3D1'}, {'id': 'SLV-3'}, {'id': 'ASLV-D1'}, {'id': 'ASLV-D2'}, {'id':  ...
# {'customer_satellites': [{'id': 'DLR-TUBSAT', 'country': 'Germany', 'launch_date': '26-05-1999', 'mass': '45', 'launcher': 'PSLV-C2'}, ...
# {'centres': [{'id': 1, 'name': 'Semi-Conductor Laboratory (SCL)', 'Place': 'Chandigarh', 'State': 'Punjab/Haryana'}, {'id': 2, 'name': ...
```

Before moving on, notice that we did not call json.loads() to convert the string in the response object into JSON. This is because the requests package provides a response method named **.json()** that parses JSON for you. You will see both techniques used in practice, so you should be familiar with both.

### Parameters (Querystring)

Web services can be customized in a few ways. Querystring parameters are one of those ways. Parameters are placed in the URL of the web service call by adding a '?' symbol at the end of the URL and then one to many key/value pairs separated by '&' between each pair (e.g., key=value&key=value&key=value). Let’s use some examples below of free web services that require parameters.

```python
import requests

# Predict the age of a person based on their name
response = requests.get("https://api.agify.io?name=homer")
print(response.text)

# Predict the gender of a person based on their name
response = requests.get("https://api.genderize.io?name=homer")
print(response.text)

# Predict the ethnic background of a person based on their name
response = requests.get("https://api.nationalize.io?name=homer")
print(response.text)

# Get the current weather for a lat/lon location
response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=40.23&longitude=-111.66&current_weather=true")
print(response.text)

# Get your IP address
response = requests.get("https://api.ipify.org?format=json")
print(response.text)

# Get game deals from Cheapshark
response = requests.get("https://www.cheapshark.com/api/1.0/deals?storeID=1&upperPrice=15")
print(response.text)

# Output:
# {"count":2238,"name":"homer","age":73}
# {"count":9109,"name":"homer","gender":"male","probability":0.99}
# {"count":6669,"name":"homer","country":[{"country_id":"US","probability":0.1501604815598518},{"country_id":"GB","probability":0.12146996887761972},{"country_id":"TT","probability":0.07829013637114275},{"country_id":"NZ","probability":0.04305957500412852},{"country_id":"CA","probability":0.037852248941131886}]}
# {"latitude":40.230103,"longitude":-111.65572,"generationtime_ms":0.09083747863769531,"utc_offset_seconds":0,"timezone":"GMT","timezone_abbreviation":"GMT","elevation":1384.0,"current_weather_units":{"time":"iso8601","interval":"seconds","temperature":"°C","windspeed":"km/h","winddirection":"°","is_day":"","weathercode":"wmo code"},"current_weather":{"time":"2026-01-01T23:30","interval":900,"temperature":7.5,"windspeed":19.5,"winddirection":115,"is_day":1,"weathercode":3}}
# {"ip":"35.229.123.83"}
# [{"internalName":"DISCOELYSIUMTHEFINALCUT","title":"Disco Elysium - The Final Cut","metacriticLink":"\/game\/disco-elysium-the-final-cut\/","dealID":"Uk9pH81%2BgPUwbkh9YzwMnYLT%2B%2FEaktqN8AgwMr6Y2wQ%3D","storeID":"1","gameID":"227942","salePrice":"3.99","normalPrice":"39.99","isOnSale":"1","savings":"90.022506","metacriticScore":"97","steamRatingText":"Very Positive","steamRatingPercent":"92","steamRatingCount":"53822","steamAppID":"632470","releaseDate":1617062400,"lastChange":1766082481,"dealRating":"9.2","thumb":"https:\/\/shared.fastly.steamstatic.com\/store_item_assets\/steam\/apps\/632470\/capsule_231x87.jpg?t=1766855203"},...]
```

---

## 5.4 Earthquakes Example

In this case study, you will retrieve earthquake event data from the United States Geological Survey (USGS) Earthquake API. Many organizations (for example, emergency management agencies, insurers, logistics providers, and utility companies) monitor seismic activity to support risk assessment and operational planning. Your goal is to make multiple requests to a single endpoint, paginate through results using querystring parameters, assemble the returned records into a Pandas DataFrame, save the dataset to a CSV file, and create a visualization that summarizes the full dataset.

The USGS endpoint supports querystring parameters that let you filter by time window and control pagination. Two parameters are especially important here: **limit** (how many records to return per request) and **offset** (how many records to skip before returning the next page). By increasing the offset in a loop, you can retrieve the complete dataset for your chosen time window in multiple API calls.

We will use the following endpoint and request JSON in GeoJSON format:

*https://earthquake.usgs.gov/fdsnws/event/1/query*

Before you begin, choose a time range that returns a manageable number of records for classroom use (for example, a few days or a couple of weeks). If you choose a very large time range, you may retrieve thousands of events, which will take longer to download and process.

1. Make one request to verify the endpoint works and inspect the JSON structure.
1. Identify where the list of earthquake events is stored in the JSON response.
1. Write a pagination loop that repeatedly calls the endpoint using **limit** and **offset** until no more results are returned.
1. Extract a consistent set of fields from each event and append them into a Python list of dictionaries.
1. Convert the list of dictionaries into a Pandas DataFrame and save the dataset as a CSV file.
1. Create a Seaborn visualization based on the full dataset (not just one page of results).

Step 1: Make a single request and inspect the response. The code below requests events in a time window and prints the status code. Then it prints the top-level keys so you can see the structure of the returned JSON object.

```python
import requests
import json

base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

params = {
  "format": "geojson",
  "starttime": "2024-01-01",
  "endtime": "2024-01-08",
  "minmagnitude": 2.5,
  "limit": 200,
  "offset": 1
}

response = requests.get(base_url, params=params)
print(response.status_code)

json_data = response.json()
print(list(json_data.keys()))

# Output:
# 200
# ['type', 'metadata', 'features', 'bbox']
```

Step 2: Find where the earthquake records live. In the GeoJSON response, the events are stored in a list under the **features** key. Print the length of that list and preview one record so you can see what fields are available.

```python
features = json_data["features"]
print(len(features))
print(json.dumps(features[0], indent=2)[:1200])

# Output:
# 200
# {
#   "type": "Feature",
#   "properties": {
#     "mag": 2.76,
#     "place": "0 km NNW of Prattville, CA",
#     "time": 1704667606430,
#     "updated": 1710020404040,
#     "tz": null,
#     "url": "https://earthquake.usgs.gov/earthquakes/eventpage/nc73986226",
#     "detail": "https://earthquake.usgs.gov/fdsnws/event/1/query?eventid=nc73986226&format=geojson",
#     "felt": 1,
#     "cdi": 2,
#     "mmi": null,
#     "alert": null,
#     "status": "reviewed",
#     "tsunami": 0,
#     "sig": 117,
#     "net": "nc",
#     "code": "73986226",
#     "ids": ",nc73986226,us6000m2dq,",
#     "sources": ",nc,us,",
#     "types": ",dyfi,nearby-cities,origin,phase-data,scitech-link,",
#     "nst": 44,
#     "dmin": 0.03814,
#     "rms": 0.18,
#     "gap": 38,
#     "magType": "md",
#     "type": "earthquake",
#     "title": "M 2.8 - 0 km NNW of Prattville, CA"
#   },
#   "geometry": {
#     "type": "Point",
#     "coordinates": [
#       -121.1591667,
#       40.211,
#       2.61
#     ]
#   },
#   "id": "nc73986226"
# }
```

Step 3: Build a pagination loop. The idea is to request a fixed number of records per call (**limit**) and then move through the dataset by increasing **offset**. When a request returns an empty **features** list, you have reached the end of the available results for the chosen filters.

Some APIs enforce rate limits. Even when an API is free and open, you should avoid sending requests as fast as possible. In the loop below, a short sleep is included as a polite practice and to reduce the chance of temporary errors.

```python
import time

all_rows = []
limit = 200
offset = 1

while True:
  params = {
    "format": "geojson",
    "starttime": "2024-01-01",
    "endtime": "2024-01-08",
    "minmagnitude": 2.5,
    "limit": limit,
    "offset": offset
  }

  response = requests.get(base_url, params=params)

  if response.status_code != 200:
    print("Request failed with status code:", response.status_code)
    break

  page = response.json()
  features = page["features"]

  if len(features) == 0:
    break

  for f in features:
    props = f.get("properties", {})
    geom = f.get("geometry", {})
    coords = geom.get("coordinates", [None, None, None])

    row = {
      "event_id": f.get("id"),
      "time_ms": props.get("time"),
      "place": props.get("place"),
      "magnitude": props.get("mag"),
      "event_type": props.get("type"),
      "url": props.get("url"),
      "tsunami_flag": props.get("tsunami"),
      "longitude": coords[0],
      "latitude": coords[1],
      "depth_km": coords[2]
    }
    all_rows.append(row)

  offset = offset + limit
  time.sleep(0.25)

print("Total events collected:", len(all_rows))

# Output:
# Total events collected: 513
```

Step 4: Convert the results into a DataFrame and perform basic cleanup. The API provides time in milliseconds since the Unix epoch. Converting that value into a datetime makes analysis and plotting much easier.

```python
import pandas as pd

df = pd.DataFrame(all_rows)

df["time_utc"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
df.drop(columns=["time_ms"], inplace=True)

df["magnitude"] = pd.to_numeric(df["magnitude"], errors="coerce")
df["depth_km"] = pd.to_numeric(df["depth_km"], errors="coerce")

df.head()
```

![A Pandas DataFrame storing the results of the earthquake monitoring API calls.](../Images/Chapter5_images/earthquake_dataframe.png)

Step 5: Save the dataset. You now have a complete dataset built from multiple API calls. Save it to a CSV file so it can be reused for later analysis.

```python
df.to_csv("usgs_earthquakes.csv", index=False)
print(df.shape)

# Output:
#
```

Step 6: Create a visualization in Seaborn using the full dataset. The example below creates a scatter plot showing how earthquake depth relates to magnitude. Because the plot is based on the DataFrame you assembled from all pages, it summarizes the entire dataset, not just the first API response.

```python
import seaborn as sns
from matplotlib import pyplot as plt

plot_df = df.dropna(subset=["magnitude", "depth_km"])

plt.figure(figsize=(10, 6))
sns.scatterplot(data=plot_df, x="magnitude", y="depth_km")
plt.title("USGS Earthquakes: Depth vs. Magnitude")
plt.xlabel("Magnitude")
plt.ylabel("Depth (km)")
plt.show()
```

![A scatterplot build from the earthquake monitoring data that plots the depth (y) of each earthquake by the magnitude (x)](../Images/Chapter5_images/earthquake_scatter.png)

At this point, you have demonstrated an end-to-end workflow for retrieving data from a web service API, handling pagination with querystring parameters, assembling a dataset, saving it, and visualizing the results. This same approach generalizes to many real-world APIs that return large datasets across multiple pages of results.

---

## 5.5 Key-Based Authentication

Free APIs are convenient for learning, but many professional-quality datasets and services require authentication. One of the most common approaches is a key-based system, where the API provider issues you a unique key and expects you to include it with each request. This allows the provider to track usage, enforce rate limits, and (when applicable) bill for service.

In this section, you will learn how to use a simple querystring API key with a popular weather service. You will create an account, confirm your email address, generate an API key, and then include that key in the URL when making a request.

Below you'll find instructions on how to sign up for an API key. This is not required. You are welcome to simply review the code below to see how the process works without running the code. Or, you can follow the instructions to generate a key for yourself to experiment with the OpenWeather API.

**Important:** After you create your account and generate your API key, it may take up to **2 hours** for the key to become active. During this activation window, you may receive a **401** status code with a message such as **"Invalid API key"** even if you copied the key correctly and confirmed your email. This is a normal delay on the provider side, not a Python error. If you receive a 401 error shortly after signing up, wait 15–30 minutes and try again, and continue retrying periodically for up to 2 hours. It took me ~20 minutes before my new key worked.

### Querystring/Parameter Keys

The simplest form of authentication involves placing a key or token in the URL querystring as a **request parameter** — Data sent to a URL in the querystring in a dictionary (?key=value) format.. In this example, the weather provider uses an API key parameter named **appid**. Your request will look like a normal URL, but it will include your API key at the end.

Follow the steps below to create your account and retrieve your API key. Use the screenshots provided in the chapter to ensure your screens match what is shown.

1. **Step 1:** Create your account at the provider’s sign-up page and submit the required information.
1. **Step 2:** During registration (or immediately after), you may be asked a question similar to “How and where will you use our API?” Select the option that best matches your intended learning use (for example, educational or personal learning). Then submit the form.
1. **Step 3:** Confirm your email address. After you sign up, the provider will send a confirmation email. Open the message and click the confirmation link. Your API key may not function until email confirmation is complete.
1. **Step 4:** Log in to your dashboard and navigate to the API keys view. Copy your key exactly as shown (API keys are case-sensitive). Store it securely and do not share it publicly.

Once you have your key, you will include it in the request URL as the value of the **appid** parameter. The example below requests the current weather for Provo, Utah. Note that **q** (the city and country code) and **units** are also querystring parameters.

```python
import requests
import json

key = '****************************************'  # Paste your key over these stars; keep the quotes

# Build the request URL using querystring parameters:
q = city,country_code
units = imperial or metric
appid = your API key
url = (
  "https://api.openweathermap.org/data/2.5/weather"
  "?q=Provo,US"  # Change this to any city/state combination you'd like to try
  "&units=imperial"
  "&appid=" + key
)

response = requests.get(url)
print(response.status_code)

json_data = json.loads(response.text)
clean_data = json.dumps(json_data, indent=2)
print(clean_data)

# Output:
# 200
# {
#   "coord": {
#     "lon": -111.6585,
#     "lat": 40.2338
#   },
#   "weather": [
#     {
#       "id": 804,
#       "main": "Clouds",
#       "description": "overcast clouds",
#       "icon": "04n"
#     }
#   ],
#   "base": "stations",
#   "main": {
#     "temp": 44.04,
#     "feels_like": 42.66,
#     "temp_min": 41.25,
#     "temp_max": 46.06,
#     "pressure": 1015,
#     "humidity": 90,
#     "sea_level": 1015,
#     "grnd_level": 835
#   },
#   "visibility": 10000,
#   "wind": {
#     "speed": 3.27,
#     "deg": 112,
#     "gust": 4
#   },
#   "clouds": {
#     "all": 100
#   },
#   "dt": 1767314539,
#   "sys": {
#     "type": 2,
#     "id": 2009866,
#     "country": "US",
#     "sunrise": 1767278950,
#     "sunset": 1767312662
#   },
#   "timezone": -25200,
#   "id": 5780026,
#   "name": "Provo",
#   "cod": 200
# }
```

Placing a key in a querystring has an important security implication: the key is included in the URL. If a URL is logged, shared, or captured in plain text, the key could be exposed. In modern practice, most reputable APIs require HTTPS (secure sockets layer encryption), which encrypts the request in transit. Even so, you should treat API keys as sensitive credentials: do not post them in screenshots, GitHub repositories, or discussion forums.

If you receive a **401** status code and a message indicating an invalid API key, double-check that you copied the key correctly (no extra spaces) and that your email is confirmed. If the key is new, remember that activation may take up to **2 hours**. If the problem persists beyond that window, regenerate a new key in the dashboard and try again.

The JSON response includes many fields, but you will usually only need a subset. For example, the **main** sub-dictionary contains numeric measures such as temperature and humidity, while the **weather** list contains a human-readable description of conditions.

```python
# Extract a few useful fields from the returned JSON
city = json_data.get('name')
temp_f = json_data.get('main', {}).get('temp')
humidity = json_data.get('main', {}).get('humidity')
condition = json_data.get('weather', [{}])[0].get('description')

print(city, temp_f, humidity, condition)

# Output: Provo 44.04 90 overcast clouds
```

What is the implication of placing the key in the querystring parameter? If the request were sent over an insecure connection (http instead of https), the full URL—including the key—could potentially be captured by network listeners. However, reputable API providers use SSL/TLS encryption (https), which encrypts the request so the key is not readable in transit. Even with https, you should still treat keys as secrets and avoid sharing them publicly.

As you continue through this chapter, you will learn other ways to authenticate (including headers and OAuth), how to handle failed requests more robustly, and how to retrieve larger datasets by making multiple calls and assembling the results into a DataFrame.

---

## 5.6 Header and Body Data

In the previous section, you learned how some APIs use a querystring key (added directly to the URL) for authentication. While that approach is common, many professional web services use a more secure pattern: the client sends authentication information in the request **headers** and sends input data in the request **body**. This is especially important for machine learning prediction services, where you must send structured feature values to the model and receive a prediction in return.

A widely used approach for header-based authentication is **OAuth** — An open standard for delegated access that allows clients to authenticate securely without placing secrets in the URL.. In many modern APIs, you do not include a key in the querystring. Instead, you include a token in the request headers—often as a bearer token—so the credential is not exposed in the URL. In addition, when you need to send larger or more complex inputs than simple query parameters, you place those inputs in the request body.

The example below previews the kind of API call you will need to make for your final course project: a **POST** request to a prediction endpoint. The request includes (1) a JSON-formatted request body containing feature values for one or more customers, and (2) an **Authorization** header containing a bearer token. The web service returns predictions in a JSON response, which we then parse and print.

**Note:** This example uses a pre-deployed demo web service so you can focus on the structure of the request. In your final project, you will deploy your own prediction service and generate your own key or token.

```python
import requests
import json

# Define the input data structure
data = {
  "Inputs": {
    "input1": [  # This is a list of feature sets; you can send multiple customers at a time
      { # Example of a customer's data
        "age": 19,              # Age of the customer
        "sex": "female",        # Gender of the customer
        "bmi": 27.9,            # BMI (Body Mass Index) of the customer
        "children": 0,          # Number of children the customer has
        "smoker": "yes",        # Smoking status: "yes" or "no"
        "region": "southwest"   # Geographic region: northeast, northwest, southeast, southwest
      },
      {  # Collect user inputs for another customer
        "age": int(input("What is your age? (enter a valid integer): ")),
        "sex": input("What is your gender? (female/male): "),
        "bmi": float(input("What is your BMI? (e.g., 21.3): ")),
        "children": int(input("How many children do you have? (enter an integer): ")),
        "smoker": input("Do you smoke? (enter yes or no): "),
        "region": input("Where do you live? (options: northeast, northwest, southeast, southwest): ")
      } # You could potentially add as many customers as you want to with additional dictionaries here
    ]
  }
}

# Encode the data as a JSON string
body = str.encode(json.dumps(data))

# Define the web service endpoint and API key
url = 'http://772ac08d-cced-464d-b0e9-03f59d2f1fef.westcentralus.azurecontainer.io/score'
api_key = 'qmvgoa0Yw2paT2ACX2n1D306aoEs1c8S'  # Replace with your actual API key

# Set the headers for the HTTP request
headers = {
  'Content-Type': 'application/json',  # Specify the format of the request body
  'Authorization': 'Bearer ' + api_key  # Include the API key for authorization
}

# Send the POST request to the endpoint; the result is immediately parsed as JSON for further use
req = requests.post(url=url, data=body, headers=headers).json()

# Print the raw response for debugging purposes
print('\nRaw response from the API:', req, '\n')

# Iterate through the response results to extract and display projected costs
print("Customer projected costs:")
for result in req['Results']['WebServiceOutput0']:
  # Format and right-align the printed cost value
  print(f"  Customer projected cost: ${result['Scored Labels']:>10.2f}")

# Output:
#  What is your age? (enter a valid integer): 45
#  What is your gender? (female/male): male
#  What is your BMI? (e.g., 21.3): 24.3
#  How many children do you have? (enter an integer): 3
#  Do you smoke? (enter yes or no): no
#  Where do you live? (options: northeast, northwest, southeast, southwest): southeast
#
#  Raw response from the API: {'Results': {'WebServiceOutput0': [{'Scored Labels': 25180.178081707592}, {'Scored Labels': 7992.073217857426}]}}
#
#  Customer projected costs:
#    Customer projected cost: $  25180.18
#    Customer projected cost: $   7992.07
```

Notice three important features of this request. First, the method is **POST**, which is commonly used when you are sending input data to be processed (such as feature values for a prediction). Second, the request body is a JSON document containing a list of records. This allows you to score multiple customers in a single request. Third, the authentication credential is not placed in the URL. Instead, it is included in the **Authorization** header as a bearer token, which is the pattern you will commonly see in production APIs.

In real projects, you should avoid hard-coding API keys directly in your notebook or script. A simple improvement is to store your key in a separate local file and read it at runtime. This reduces the chance of accidentally sharing the key when you submit code, upload notebooks, or copy/paste snippets.

```python
# Example: store your API key in a separate file and load it at runtime
#
# Create a file named: api_key.txt
# Put ONLY the key on a single line, like this:
# qmvgoa0Yw2paT2ACX2n1D306aoEs1c8S

with open("api_key.txt", "r") as f:
  api_key = f.read().strip()

headers = {
  "Content-Type": "application/json",
  "Authorization": "Bearer " + api_key
}

# You can now reuse 'headers' in your POST request:
# req = requests.post(url=url, data=body, headers=headers).json()
```

Later, you will learn stronger approaches (such as environment variables or secret managers). However, using a separate key file is an easy first step that keeps sensitive values out of your code while still being simple to understand and use.

---

## 5.7 Stock Market Data Example

Once you are comfortable working with APIs, a wide range of real-world data sources becomes accessible—including financial markets. In this example, you will retrieve historical stock price data using the polygon.io API. Polygon provides professional-grade market data through a well-documented REST API and is widely used in analytics, trading, and fintech applications.

To follow along, you will need to create a free Polygon account and generate an API key. The free tier is sufficient for this example and does not require any payment information. You will use the API key as a bearer token in the request headers, reinforcing the header-based authentication pattern introduced earlier in this chapter.

The video below demonstrates how to make authenticated requests to Polygon’s API and interpret the returned JSON data. After watching the video, review the provided code examples to see how the same logic can be implemented directly in Python.

The first example below demonstrates how to retrieve daily aggregate price data for a single stock ticker (Apple, AAPL) over a short date range. Notice that the API key is passed in the request headers rather than in the querystring.

```python
import json, requests

key = "yLJlbno91EWcqYDh3Mg8DQUgwJ4qG7ur"  # Replace with your own key
headers = {"Authorization": "Bearer " + key}

url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-08-17/2023-08-18?adjusted=true&sort=asc&limit=120"

req = requests.get(url, headers=headers)
result = json.loads(req.text)
result

# Output:
# {
#   'ticker': 'AAPL',
#   'queryCount': 1,
#   'resultsCount': 1,
#   'adjusted': True,
#   'results': [
#     {
#       'v': 66054482.0,
#       'vw': 174.5857,
#       'o': 177.14,
#       'c': 174.0,
#       'h': 177.5054,
#       'l': 173.48,
#       't': 1692244800000,
#       'n': 673083
#     }
#   ],
#   'status': 'DELAYED',
#   'request_id': '...',
#   'count': 1
# }
```

The response contains both metadata and a **results** list with price and volume fields such as open, high, low, close, and volume. These values are timestamped and suitable for time-series analysis.

The next example expands on this idea by making multiple API calls—one per trading day—and appending the results into a Pandas DataFrame. This pattern is common when building datasets from APIs that return one day (or one page) of data per request.

```python
import json, requests, pandas as pd

dates = ['2023-08-01', '2023-08-02', '2023-08-03', '2023-08-04', '2023-08-05']

key = "yLJlbno91EWcqYDh3Mg8DQUgwJ4qG7ur"  # Replace with your own key
headers = {"Authorization": "Bearer " + key}

df = pd.DataFrame(columns=[
  'date', 'symbol', 'open', 'high', 'low', 'close',
  'volume', 'afterHours', 'preMarket'
])

for date in dates:
  url = f"https://api.polygon.io/v1/open-close/AAPL/{date}?adjusted=true"
  req = requests.get(url, headers=headers)
  result = json.loads(req.text)
  try:
    df.loc[len(df)] = [
      result['from'], result['symbol'], result['open'], result['high'],
      result['low'], result['close'], result['volume'],
      result['afterHours'], result['preMarket']
    ]
  except:
    print(result)

df

# Output:
# {'status': 'NOT_FOUND', 'request_id': '69dcb26ce3eac136ad58422b9d660428', 'message': 'Data not found.'}
# Note that you'll only get that message above if you attempt more than 5 API calls in a minute
```

If you exceed the free-tier rate limit, the API may return an error message instead of data. This highlights the importance of handling exceptions when working with production APIs. After building the DataFrame, you can export the data to a CSV file, visualize trends, or integrate the dataset into a larger analytics workflow.

![Stock Market Data Example](../Images/Chapter5_images/stock_results.png)

---

## 5.8 ESPN NBA Example

In many real-world analytics workflows, you are not interested in upcoming (scheduled) events—you want completed events with final scores and reliable statistics. In this case study, you will use a free ESPN endpoint (no registration and no API key required) to retrieve NBA game data, filter to games that have already been played, and then enrich your dataset with team-level statistics from a second endpoint.

You will build two datasets: (1) a **games** table with one row per completed game, and (2) a **team_stats** table with two rows per game (one row per team). You will then save the results to .CSV files and create a visualization in Seaborn.

These ESPN endpoints are publicly accessible, but they are not officially documented and may change over time. Always check the **status_code** and inspect the JSON structure before assuming it will always look the same.

The primary endpoint for the NBA scoreboard is shown below. By default, it can include a mixture of completed games, in-progress games, and scheduled games (including games that have not happened yet).

**Scoreboard endpoint:** https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard

**Scoreboard with date filter:** https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=YYYYMMDD

**Game summary endpoint (per game):** https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=EVENT_ID

#### Step 1: Inspect the Default Scoreboard Response

Before applying any filters, first call the scoreboard endpoint without parameters. This allows you to confirm that scheduled games are returned by default and explains why additional filtering is necessary.

```python
import requests
import json

url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
response = requests.get(url)

print(response.status_code)

json_data = json.loads(response.text)

# Show the first event so we can inspect its status.
events = json_data.get("events", [])
events[0]

# Output:
# 200
# {'id': '401810331',
#  'uid': 's:40~l:46~e:401810331',
#  'date': '2026-01-03T00:00Z',
#  'name': 'San Antonio Spurs at Indiana Pacers',
#  ...
#  'status': {'type': {'name': 'STATUS_SCHEDULED',
#                      'state': 'pre',
#                      'completed': False,
#                      'description': 'Scheduled'}}}
```

#### Step 2: Retrieve the Last 200 Completed Games

The scoreboard endpoint supports a **dates** querystring parameter. We treat each date as a “page” of results, walking backward day-by-day and collecting only completed games until we reach 200.

Completed games typically have a status **state** value of **post**. Filtering on this value ensures that scores and statistics are final.

```python
import datetime as dt
import time
import pandas as pd
import requests

BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

def fetch_scoreboard(date_yyyymmdd):
  url = BASE + "?dates=" + date_yyyymmdd
  r = requests.get(url)
  if r.status_code != 200:
    return None, r.status_code
  return r.json(), 200

completed_games = []
target_n = 200

d = dt.datetime.now(dt.UTC).date()

while len(completed_games) < target_n:
  date_str = d.strftime("%Y%m%d")
  data, status = fetch_scoreboard(date_str)

  if status == 200 and data:
    for e in data.get("events", []):
      if e.get("status", {}).get("type", {}).get("state") == "post":
        completed_games.append(e)

  d = d - dt.timedelta(days=1)
  time.sleep(0.25)

completed_games = completed_games[:target_n]

print("Completed games collected:", len(completed_games))
print("Example event id:", completed_games[0].get("id"))

# Output:
# Completed games collected: 200
# Example event id: 401810326
```

#### Step 3: Create the Games DataFrame

Now convert the list of completed games into a structured DataFrame with one row per game.

```python
games_df = pd.DataFrame(columns=[
  "event_id",
  "date_utc",
  "game_name",
  "status",
  "home_team",
  "away_team",
  "home_score",
  "away_score",
  "venue"
])

for e in completed_games:
  event_id = e.get("id")
  date_utc = e.get("date")
  game_name = e.get("name")

  status_obj = e.get("status", {}).get("type", {})
  status = status_obj.get("description")

  competitions = e.get("competitions", [])
  if len(competitions) == 0:
    continue

  comp = competitions[0]
  venue = comp.get("venue", {}).get("fullName")

  competitors = comp.get("competitors", [])
  home = next((c for c in competitors if c.get("homeAway") == "home"), None)
  away = next((c for c in competitors if c.get("homeAway") == "away"), None)

  if home is None or away is None:
    continue

  home_team = home.get("team", {}).get("displayName")
  away_team = away.get("team", {}).get("displayName")

  try:
    home_score = int(home.get("score", 0))
  except:
    home_score = None

  try:
    away_score = int(away.get("score", 0))
  except:
    away_score = None

  games_df.loc[len(games_df)] = [
    event_id,
    date_utc,
    game_name,
    status,
    home_team,
    away_team,
    home_score,
    away_score,
    venue
  ]

games_df.head()
```

#### Step 4: Enrich the Dataset with Team Statistics

Enrich your dataset by calling the **summary** endpoint for each completed game. This produces a second DataFrame with one row per team per game (two rows per game). Because this step makes many requests (up to 200), include a delay so you do not overwhelm the service.

Before you write a loop that requests 200 games and extracts team statistics, you should first inspect the JSON structure returned by the ESPN _summary_ endpoint for a single event. This is an important habit when working with APIs because the data is often nested and the keys may not match what you expect. By printing a small piece of the response (instead of the entire JSON), you can confirm where the team information and the team statistics are located and what the statistics objects look like.

In the code below, we select the first **event_id** from _games_df_, call the summary endpoint, and then extract the first team object in the boxscore. We print the team name and the first five entries in the team’s statistics list. This will show you which keys the API uses for statistics (for example, you may see fields such as **abbreviation**, **name**, and **displayValue**). Once you understand this structure, you can confidently write code to extract the statistics you need for every team in every game.

```python
event_id = games_df.loc[0, "event_id"]
summary_url = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=" + str(event_id)
summary = requests.get(summary_url).json()

team0 = summary["boxscore"]["teams"][0]
team0["team"]["displayName"], team0["statistics"][:5]

# Output:
# ('Houston Rockets',
#  [{'name': 'fieldGoalsMade-fieldGoalsAttempted',
#    'displayValue': '47-82',
#    'label': 'FG'},
#   {'name': 'fieldGoalPct',
#    'displayValue': '57',
#    'abbreviation': 'FG%',
#    'label': 'Field Goal %'},
#   {'name': 'threePointFieldGoalsMade-threePointFieldGoalsAttempted',
#    'displayValue': '12-29',
#    'label': '3PT'},
#   {'name': 'threePointFieldGoalPct',
#    'displayValue': '41',
#    'abbreviation': '3P%',
#    'label': 'Three Point %'},
#   {'name': 'freeThrowsMade-freeThrowsAttempted',
#    'displayValue': '14-18',
#    'label': 'FT'}])
```

Notice that it includes some summary statistics, but not all that we might expect. For example, the score or total points scored by the team is not included in the team statistics list. This happens because ESPN often stores points in a different part of the JSON (typically in the header competitors list), while the boxscore statistics list focuses on shooting, rebounds, assists, turnovers, and related metrics. This is common in real APIs: values you logically group together may be stored in different branches of the JSON structure.

```python
import time
import requests
import pandas as pd

team_stats_df = pd.DataFrame(columns=[
  "event_id",
  "date_utc",
  "team",
  "home_away",
  "points",
  "fg",
  "fg_pct",
  "three_pt",
  "three_pt_pct",
  "ft",
  "ft_pct",
  "rebounds",
  "assists",
  "turnovers"
])

for idx, row in games_df.iterrows():
  event_id = row["event_id"]
  date_utc = row["date_utc"]

  summary_url = (
    "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
    "?event=" + str(event_id)
  )
  res = requests.get(summary_url)

  if res.status_code != 200:
    print("Skipping event", event_id, "status", res.status_code)
    time.sleep(0.35)
    continue

  summary = res.json()

  # 1) Build a points lookup from the HEADER (most reliable location for points).
  #    This is usually present for completed games even when boxscore stats vary.
  points_by_team_id = {}
  try:
    competitors = summary["header"]["competitions"][0]["competitors"]
    for c in competitors:
      team_id = c.get("team", {}).get("id")
      score = c.get("score")  # usually a string like "112"
      if team_id is not None:
        points_by_team_id[str(team_id)] = score
  except Exception:
    competitors = []

  # 2) Pull team rows from the BOXSCORE and match to header points using team id.
  teams = summary.get("boxscore", {}).get("teams", [])
  if not teams:
    print("No boxscore teams found for event", event_id)
    time.sleep(0.35)
    continue

  for t in teams:
    team_obj = t.get("team", {})
    team_name = team_obj.get("displayName")
    home_away = t.get("homeAway")

    team_id = team_obj.get("id")
    points = points_by_team_id.get(str(team_id)) if team_id is not None else None

    # Parse the rest from statistics (keep it explicit)
    stats_list = t.get("statistics", [])
    stats = {}
    for s in stats_list:
      key = s.get("abbreviation") or s.get("label") or s.get("name")
      if key is None:
        continue

      val = s.get("displayValue")
      if val is None:
        val = s.get("value")

      stats[str(key).upper()] = val

    fg = stats.get("FG")                                  # "47-82"
    fg_pct = stats.get("FG%")                             # "57"
    three_pt = stats.get("3PT") or stats.get("3P")        # "12-29"
    three_pt_pct = stats.get("3P%") or stats.get("3PT%")  # "41"
    ft = stats.get("FT")                                  # "14-18"
    ft_pct = stats.get("FT%")                             # sometimes missing
    rebounds = stats.get("REB") or stats.get("TRB")
    assists = stats.get("AST")
    turnovers = stats.get("TO") or stats.get("TOV")

    team_stats_df.loc[len(team_stats_df)] = [
      event_id,
      date_utc,
      team_name,
      home_away,
      points,
      fg,
      fg_pct,
      three_pt,
      three_pt_pct,
      ft,
      ft_pct,
      rebounds,
      assists,
      turnovers
    ]

  time.sleep(0.35)

team_stats_df.head()
```

![A Pandas DataFrame of NBA games with summary statistics merged with the original game data.](../Images/Chapter5_images/nba_results2.png)

#### Step 5: Save Your Datasets to CSV Files

Save both datasets to .CSV files so you can reuse them for analysis without calling the API again.

```python
games_df.to_csv("espn_nba_games_last200.csv", index=False)
team_stats_df.to_csv("espn_nba_team_stats_last200.csv", index=False)

print("Saved files:")
print(" - espn_nba_games_last200.csv")
print(" - espn_nba_team_stats_last200.csv")

# Output:
# Saved files:
#  - espn_nba_games_last200.csv
#  - espn_nba_team_stats_last200.csv
```

#### Step 6: Visualize the Results in Seaborn

Create a visualization in Seaborn. The example below converts the **points** column to numeric and plots the average points by team. Because the full dataset is large, we summarize by team and then chart only the top 20 teams by average points over the collected games.

```python
import seaborn as sns
from matplotlib import pyplot as plt

team_stats_df["points_num"] = pd.to_numeric(team_stats_df["points"], errors="coerce")

plot_df = (
  team_stats_df
    .dropna(subset=["points_num"])
    .groupby("team", as_index=False)["points_num"]
    .mean()
)

# Optional: keep chart readable
plot_df = plot_df.sort_values("points_num", ascending=False).head(20)

plt.figure(figsize=(14, 6))
ax = sns.barplot(data=plot_df, x="team", y="points_num")

plt.xticks(rotation=45, ha="right")
plt.title(f"ESPN NBA: Average Team Points (Last {target_n} Games)")
plt.xlabel("Team")
plt.ylabel("Average Points")
plt.tight_layout()
plt.show()

# Output:
# (Your chart will appear here.)
```

You have now demonstrated a realistic API workflow: (1) confirm what the API returns by default (including scheduled games), (2) apply querystring filtering with **dates**, (3) “paginate” by looping over multiple dates until you collect enough completed games, (4) enrich each game by making additional API calls, (5) store results in DataFrames and export them, and (6) visualize the final dataset.

### Free ESPN Sports Data APIs

If you like this area of sports analytics, ESPN provides a collection of undocumented but publicly accessible web service endpoints that return structured JSON data without requiring API keys, authentication, or student registration. These APIs are commonly used for educational projects, sports analytics prototypes, dashboards, and exploratory data analysis. While not officially guaranteed, they are stable enough for instructional use and allow students to work with real, up-to-date sports data.

These endpoints follow REST-style conventions and return nested JSON structures. They are read-only, rate-limited implicitly, and intended for data retrieval rather than transactional use. Because they are undocumented, students should always inspect the JSON structure before writing full data pipelines.

---

## 5.9 Practice Data Retrieval

Optionally, it may help to practice the concepts you learned in the last few chapters:

### 5.9 Practice: Data Retrieval

- This assignment should be completed individually.
- Perform each of the specific tasks found in the questions below.
- Use the template .ipynb file provided below to complete each task.

- Upload the .ipynb file you used to collect this data.

---

## 5.10 Assignment

Complete the assignment below:

### 5.10 Web Service APIs: Pokemon

- The popularity of the Pokémon character
- Rarity and evolutionary stage
- Combat attributes (e.g., base stats, abilities, types)
- Nostalgia and franchise prominence

- Identifying high-value Pokémon for acquisition
- Comparing Pokémon across generations and types
- Enriching pricing models with objective performance attributes
- Supporting future dashboards, models, or valuation tools

- **API Base URL:** https://pokeapi.co/api/v2/
- **Official Documentation:** https://pokeapi.co/docs/v2

- Work with **REST APIs** using Python
- Handle **pagination** and looping over endpoints
- Parse **nested JSON structures**
- Build and expand a DataFrame incrementally
- Store collected data in a reusable, structured format

- Written task instructions in Markdown cells
- Code cells marked with # Question [n] for each task

- All required API calls are executed successfully
- All cells are executed before submission

---
