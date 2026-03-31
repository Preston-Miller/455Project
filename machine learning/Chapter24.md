# Chapter 24: Image Classification

## Learning Objectives

- Students will be able to apply optical character recognition (OCR) using PyTesseract to extract text from images
- Students will be able to use pre-trained Haar cascade models in OpenCV for face and feature detection in images
- Students will be able to explain the basic architecture of neural networks for image classification tasks

---

## 24.1 Introduction

Image classification has become one of the hottest topics in machine learning—thanks in large part to the excellent Python packages that have emerged. Tensorflow was created by Google and includes many of the algorithms found in scikit-learn, in addition to many advancements in neural network algorithms for training image classifiers. PyTorch was created by Facebook and may be easier to learn than Tensorflow. However, PyTorch learning materials are still not as advanced since it is a newer package than Tensorflow. Both packages have advantages and disadvantages.

We will focus first on using existing image classifiers for general objects (e.g., faces, smiles, bodies) and then on optical character recognition (OCR) from packages that include trained models.

### Examples

#### Photo Classifier

One of the most common image classifiers that most people use every day is on your mobile device. The advanced organization of images by Apple and Android devices is made possible by training unique image classification models on each device. In the image below, iOS has classified all images containing a particular person into a single folder using this technique:

![Apple Photos App Image Classification](../Images/Chapter24_images/apple_photos.png)

#### Facial Recognition for Citizen Tracking

Facial recognition is also used by governments to track crime and citizens. While the US has used facial recognition to identify petty thieves, violent criminals, and other potential threats, China has used it much more extensively, with cameras on virtually every street corner (see image below).1

#### Product Discoverability

A commercial use of image recognition is to facilitate shopping by image. With this feature, consumers can take a photo of a product they are interested in and use it to find the product’s price across retailers and to discover related products. Imagga is a platform that has trained models for businesses to predict from for such purposes.

---

## 24.2 Neural Networks (Deep Learning)

Image processing and classification is typically performed using deep learning techniques based on the neural network algorithm. The image below summarizes the basic structure of the algorithm:

![Basic Neural Network Structure. Neural Network Structure  Input Layer has 5 inputs that map to three objects in a hidden layer. These three objects then go to a single object in the output layer. Under this drawing is an equation of the same process with the inputs labeled as X and Weights and then Bias as Sigma and activate function as f, leading finally to Y.](../Images/Chapter24_images/NN diagram.png)

We do not have time for a thorough discussion of neural networks in this class, but there are many good introductory YouTube videos (e.g., Neural Networks Pt. 1: Inside the Black Box by StatQuest) and free online articles (e.g., Everything you need to know about Neural Networks and Backpropagation).

Basically, neural network algorithms—including convolutional networks, deep learning networks, and tangential networks—are optimal for image classification tasks because they are excellent at automatically generating and selecting features based on the color composition of an image. Consider how much time we spent earlier to determine the best set of features in an MLR algorithm when the features were simple and obvious. That task is infinitely harder when using an image as the sole feature. By creating dynamic numbers of nodes and hidden layers, neural networks simplify and automate the feature selection process.

---

## 24.3 OCR

One of the most basic forms of image processing is **optical character recognition (OCR)** — The identification of printed characters using photoelectric devices and computer software.. OCR is the identification of printed characters using photoelectric devices and computer software. PyTesseract is a great, simple package that supports OCR from digital images. Let’s learn to use it. Begin by installing the needed packages:

```python
# If using on Colab:
!sudo apt install tesseract-ocr
!pip install pytesseract

# If using on your own machine, install the .exe file here: https://github.com/UB-Mannheim/tesseract/wiki and you do not need to install the !sudo apt install tesseract-ocr
```

Next, we need some images to process. Download the zipped folder below, unzip it, and place it in the same folder as your .ipynb file. Once there, import the packages below.

```python
import pytesseract
from pathlib import Path
```

Next, the basic idea is to open the folder full of images, read each image, and then run the .image_to_string(file_name) function of PyTesseract. If you would like to follow along, you can find the same images used in this tutorial below:

```python
import pytesseract

# Use this if installed on your local computer
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract'

from pathlib import Path

pathlist = Path('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/').glob('*.*')
for path in pathlist:
  path_in_str = str(path)
  try:
    print(f'{path_in_str}:\n{pytesseract.image_to_string(path_in_str)}')
  except:
    print(f'{path_in_str}')

# Output:
# /content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/3_1238858492653551617.jpg:
# /content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/3_1240664487151181824.jpg:
# /content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/3_1241934859922223105.jpg:
# /content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/3_1243139422608113664.jpg:
# /content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/3_1243174640165572609.jpg:
# How effective is vaccination?

# Diphtheria cases | Introduction of vaccine

# I I l
# 1910 1940 1965

# Pertussis cases Introduction of vaccine

# ...
# [goes on much longer]
```

You can see the text data flowing in the output. But to use this information and perform natural language processing, we need to store it in a DataFrame. See if you can modify that code on your own to place the results in a DataFrame. If you have trouble, you can refer to the example below:

```python
import pandas as pd
import re

df = pd.DataFrame(columns=['text'])

# Change this to your path. If you are working on your own machine, then the path will simply be 'images/'
pathlist = Path('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/').glob('*.*')

for path in pathlist:
  try:
    text = pytesseract.image_to_string(str(path))
  except:
    text = ""

  # The re package allows us to use regex to remove tabs, extra spaces, and line breaks
  df.loc[path.name] = [re.sub('\s+',' ', text)]

df.sort_values(by=['text'], ascending=False).head()
```

![Table with five records and columns for image and text.](../Images/Chapter24_images/ocr_df.png)

---

## 24.4 Extract Entities with OpenCV

![Eight glasses with a different kind of green leaf in each.](../Images/Chapter24_images/extraction.png)

Let’s take image classification a step further by using a prior trained model to identify entities from images. OpenCV includes several trained models for identifying faces, people, eyes, smiles, bodies, etc. Let’s begin by installing the package:

```python
!pip install opencv-python
```

Now import the packages:

```python
import cv2
from pathlib import Path

# Needed if using Colab
from google.colab.patches import cv2_imshow
```

Now we will import the pretrained model for identifying faces. The model is stored in a .xml file that is available to download from a git repository. You will find the latest versions there. However, we have also made a copy of them for you to download. Once you have downloaded the zipped folder below, decompress (unzip) it and place the folder in the same location as the .ipynb file you are working from in this tutorial.

Now we will create a trained model object to classify against:

```python
face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
```

Next, let’s set up a loop to go through each image. To keep things simple, let’s enumerate through the list so that we can end after the first five images as we test out the classifier. This method will save time going through all images.

```python
pathlist = Path('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/').glob('*.*')
for i, path in enumerate(pathlist):
  if i > 4:
    break

  path_in_str = str(path)

  # Read the input image
  img = cv2.imread(path_in_str)

  # Convert into grayscale
  gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

  # Detect faces
  faces = face_cascade.detectMultiScale(image=gray, scaleFactor=1.3, minNeighbors=5)

  # Draw rectangle around the faces
  for (x, y, w, h) in faces:
    cv2.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2)

  # Display the output
  try: # It will try this first, but it only works from your own machine
    cv2.imshow('img', img)
    cv2.waitKey()
  except: # If you are on Google Colab, do this:
    cv2_imshow(img)

  # status = cv2.imwrite('faces_detected.jpg', img) # We can save the images that have entities somewhere
  print(f"{id}: {len(faces)} faces")
```

There are three important inputs to understand here:

- **image**: The image object being examined. It must be in grayscale for the purposes of these pretrained models.
- **scaleFactor**: The scale factor is used to create your scale pyramid. The value refers to how much the image size is reduced at each image scale. Your model has a fixed size defined during training, which is visible in the XML. One face of this fixed size is detected in the image. Rescaling the input image—by setting this parameter—will resize a larger face to a smaller one, thus making it detectable by the algorithm. A value of 1.3 means that your image will be reduced by 30%—increasing your chance of finding a matching size. This also means that the algorithm works slower since it is more thorough. Higher numbers may increase the speed but risk missing some faces altogether. Good values for this parameter are between 1.05 and 1.4. You should adjust the scale factor with a sample of the data until it appears to most accurately capture all faces.
- **minNeighbors**: The number of neighbors each candidate rectangle should have. This parameter will affect the quality of the detected faces. A higher value results in fewer detections but yields higher quality. A general range of good values is between 3 and 6.

Next, on your own, see if you can create a code block to detect eyes. There are trained classification models (i.e., _haarcascade_ files) available through the OpenCV package to detect a variety of features on a person, including eyes, eyes with glasses, faces from various angles, ears, mouth, nose, and upper/lower/full bodies. You can download all of the trained model .xml files below. If you have any trouble detecting eyes, you can copy and study the code below.

```python
# Complete list of pre-trained classifiers available in OpenCV:
# haarcascade_eye_tree_eyeglasses.xml   haarcascade_mcs_leftear.xml
# haarcascade_eye.xml                   haarcascade_mcs_lefteye.xml
# haarcascade_frontalface_alt2.xml      haarcascade_mcs_mouth.xml
# haarcascade_frontalface_alt_tree.xml  haarcascade_mcs_nose.xml
# haarcascade_frontalface_alt.xml       haarcascade_mcs_rightear.xml
# haarcascade_frontalface_default.xml   haarcascade_mcs_righteye.xml
# haarcascade_fullbody.xml              haarcascade_mcs_upperbody.xml
# haarcascade_lefteye_2splits.xml       haarcascade_profileface.xml
# haarcascade_lowerbody.xml             haarcascade_righteye_2splits.xml
# haarcascade_mcs_eyepair_big.xml       haarcascade_smile.xml
# haarcascade_mcs_eyepair_small.xml     haarcascade_upperbody.xml
```

```python
face_cascade = cv2.CascadeClassifier('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/haarcascade_frontalface_default.xml')
eye_cascade = cv2.CascadeClassifier('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/haarcascade_eye.xml')

# Change this to your path. If you are working on your own machine, then the path will simply be 'images/'
pathlist = Path('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/').glob('*.*')
for i, path in enumerate(pathlist):
  if i > 4: # Just to keep this loop short; remove
    break

  path_in_str = str(path)

  # Read the input image
  img = cv2.imread(path_in_str)

  # Convert into grayscale
  gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

  # Detect faces
  faces = face_cascade.detectMultiScale(image=gray, scaleFactor=1.05, minNeighbors=3)

  # Detect eyes
  eyes = eye_cascade.detectMultiScale(image=gray, scaleFactor=1.05, minNeighbors=3)

  # Draw rectangle around the faces
  for (x, y, w, h) in faces:
    cv2.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2)

  # Draw rectangle around the eyes
  for (x, y, w, h) in eyes:
    cv2.rectangle(img, (x, y), (x+w, y+h), (0, 255, 0), 2)

  # Display the output
  try:
    cv2.imshow('img', img)
    cv2.waitKey()
  except:
    cv2_imshow(img)

  # status = cv2.imwrite('faces_detected.jpg', img) # We can save the images that have entities somewhere
  print(f"{id}: {len(faces)} faces")
```

Notice that we have made the eye detector so sensitive that it thinks everything is an eye. Try adjusting the scaleFactor and minNeighbors attributes to get a more accurate classification. Lastly, let’s combine our OCR and image classifier into a single loop and store all of the results in the DataFrame we created previously. See if you can do that on your own before examining the code below:

```python
df = pd.DataFrame(columns=['image', 'text', 'faces', 'eyes'])

# Load the cascade
face_cascade = cv2.CascadeClassifier('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/haarcascade_frontalface_default.xml')
eye_cascade = cv2.CascadeClassifier('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/haarcascade_eye.xml')

# Change this to your path. If you are working on your own machine, then the path will simply be 'images/'
pathlist = Path('/content/drive/MyDrive/Colab Notebooks/code_samples/ImageProcessing/images/').glob('*.*')

for i, path in enumerate(pathlist):
  path_in_str = str(path)

  # OCR the text
  try:
    text = pytesseract.image_to_string(path_in_str)
  except:
    text = ""

  # Read the input image
  img = cv2.imread(path_in_str)

  faces = []
  eyes = []

  try:
    # Convert into grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Detect faces
    faces = face_cascade.detectMultiScale(gray, 1.3, 4)
    eyes = eye_cascade.detectMultiScale(gray, 1.3, 4)

    # Draw rectangle around the faces
    for (x, y, w, h) in faces:
      cv2.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2) # Blue

    # Draw rectangle around the eyes
    for (x, y, w, h) in eyes:
      cv2.rectangle(img, (x, y), (x+w, y+h), (0, 255, 0), 2) # Green
  except:
    pass

  # Display the output
  try:
    cv2.imshow('img', img)
    cv2.waitKey()
  except:
    try:
      cv2_imshow(img)
    except:
      pass

  df.loc[i] = [id, re.sub('\s+',' ', text), len(faces), len(eyes)]

df.to_csv('image_text.csv')
df.sort_values(by=['text'], ascending=False).head()
```

Notice that even after you modify the parameters, OpenCV does not perfectly recognize every eye and face. That does not mean that the classification is not useful. As with all predictive models, there will be a certain level of inaccuracy. Otherwise, there would be no need for a predictive model. If you suspect too much error in the faces and eyes predictions, you could always recode those features as true/false to simply represent whether any faces or eyes are found. While there would still be a certain level of error in that coding, it would be more accurate than the count we created.

Finally, notice that I added another try/except block in there on the off chance that some of the images could not be converted into grayscale or threw some other error. When analyzing other people’s content, you never know what you might get. So it is useful to be prepared so that the entire routing does not break.

### Additional Resources

- OpenCV haarcascade XML files (trained classification models)

---

## 24.5 Pre-Trained Classifiers

While OpenCV is a great open-source option to get started with image classification, there are many good proprietary tools available. The large cloud vendors (Amazon AWS “Rekognition” and Azure “Computer Vision API”) each offer high-end, easy-to-use APIs that can retrieve far more accurate results than OpenCV without the need for you to figure out the optimal tuning. However, there are also many smaller and less-expensive vendors that offer equally accurate data. Let’s get some practice with one example vendor: Imagga. Go to the Imagga Website and click the “Get Free API Key” link on the home page:

Fill out the registration form with something like the following:

Complete any additional steps necessary to register, then navigate to the developer dashboard.

![Imagga User Dashboard with API usage details and an option for more API Documentation.](../Images/Chapter24_images/imagga_dashboard.png)

Now that you have an API key, the API secret, and authorization codes, let’s try out the endpoint. First, let’s store our authorization credentials to use later:

```python
# Enter your own info here. If you use mine, it will run out
api_key:        '***************'
api_secret:     '***************************'
authorization:  'Basic *******************************************************'
endpoint =      'https://api.imagga.com'
```

Next, we need to post the images to a valid URL in order to send them to the Imagga endpoint. I have posted two sample images to my server that you can use in the format below. Basically, we need to pass in the URL of the folder where the images are located and a list of image names to process from that folder:

```python
import requests
import json

url = 'https://api.imagga.com/v2/tags/?image_url=https://www.ishelp.info/data/images/'
images = ['3_628913364622688256.jpg', '3_628933195636047872.jpg']

for image in images:
  request = requests.get(url + image, auth=(api_key, api_secret))
  json_data = json.loads(request.text)
  clean_data = json.dumps(json_data, indent=2)
  print(f"{clean_data}")

# tag = the entity found in the image
# confidence = ranges from 0 to 100

# Output:
# {
#    "result": {
#      "tags": [
#        {
#          "confidence": 31.8638324737549,
#          "tag": {
#            "en": "people"
#          }
#        },
#        {
#          "confidence": 31.5081024169922,
#          "tag": {
#            "en": "person"
#          }
#        },
#        {
#          "confidence": 29.9828624725342,
#          "tag": {
#            "en": "adult"
#          }
#        },
#        {
#          "confidence": 28.2731456756592,
#          "tag": {
#            "en": "smiling"
#          }
#        },
#        {
#          "confidence": 27.2529125213623,
#          "tag": {
#            "en": "nurse"
#          }
#        },
#        {
#          "confidence": 26.6403732299805,
#          "tag": {
#            "en": "attractive"
#          }
#        },
```

I only included a sample of all of the recognized tags from the photos. If you run the code and examine it in your notebook, you will see that many more tags were recognized. Basically, you could create a new feature for each tag that you cared about and store these scores as feature values for each row. Let’s see a list of the general categories offered by the Imagga endpoint:

```python
response = requests.get('https://api.imagga.com/v2/categorizers', auth=(api_key, api_secret))
json_data = json.loads(response.text)
clean_data = json.dumps(json_data, indent=2)
print(f"{clean_data}")
```

Notice that there is a general_v3 category followed by a "nsfw" category. This is very useful if you allow customers to upload their own photos. You can tag photos as "nsfw" and prevent the user from uploading them.

Finally, let’s store the image classification scores (i.e., the likelihood that the image contains the tag) in a DataFrame to use later for modeling:

```python
import pandas as pd

df = pd.DataFrame(columns=["interior objects", "nature landscape", "beaches seaside", "events parties", "food drinks",
                          "paintings art", "pets animals", "text visuals", "sunrises sunsets", "cars vehicles",
                          "macro flowers", "streetview architecture", "people portraits"])

url = 'https://api.imagga.com/v2/categories/personal_photos/?image_url=https://www.ishelp.info/data/images/'
images = ['3_628913364622688256.jpg', '3_628933195636047872.jpg']

for image in images:
  request = requests.get(url + image, auth=(api_key, api_secret))
  json_data = json.loads(request.text)
  print(json.dumps(json_data, indent=2))

  # Create a list of 0.0 scores to update as we get data for each category we want to score in our DataFrame
  scores = [0.0] * len(df.columns)

  # Iterate through each category of the result
  for category in json_data["result"]["categories"]:
    # Find the associated column in the DataFrame
    for n, col in enumerate(df.columns):
      if col == category['name']['en']:
        # Store the score
        scores[n] = category['confidence']
        break # No need to keep looping once we’ve found the score

    # Store the list as a new row in the DataFrame
    df.loc[image] = scores

df

# Output:
# {
#    "result": {
#      "categories": [
#        {
#          "confidence": 78.2220153808594,
#          "name": {
#            "en": "people portraits"
#          }
#        },
#        {
#          "confidence": 16.2230091094971,
#          "name": {
#            "en": "paintings art"
#          }
#        },
#        {
#          "confidence": 2.56214904785156,
#          "name": {
#            "en": "pets animals"
#          }
#        }
#      ]
#    },
#    "status": {
#      "text": "",
#      "type": "success"
#    }
#  }
#  {
#    "result": {
#      "categories": [
#        {
#          "confidence": 73.7639312744141,
#          "name": {
#            "en": "people portraits"
#          }
#        },
#        {
#          "confidence": 24.0428619384766,
#          "name": {
#            "en": "paintings art"
#          }
#        },
#        {
#          "confidence": 2.0138156414032,
#          "name": {
#            "en": "pets animals"
#          }
#        }
#      ]
#    },
#    "status": {
#      "text": "",
#      "type": "success"
#    }
#  }
```

---

## 24.6 Practice

Consider working through these practice problems:

### 24.6 Practice: Image Classification: Concepts

---

## 24.7 Assignment

Complete the assignment(s) below (if any):

---
