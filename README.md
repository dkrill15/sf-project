# Data Collection for YOLOv5 Models

## cv_training

This folder contains code adapted from [this repository](https://github.com/yoloso/urbanchange). The two main scripts 1) generate all street segments for a given bounding box and 2) pull Google Street View images from various headings along those segments.
To access GSV API, add a CONFIG.py file in that folder containing the line : `SV_api_key="your-api-key"`

## street_segments, property_data

These folder contain the preprocessing code that builds the database for both property value data and street segment data. Contact the repository owner for access to these datasets.

## find_nearest.py, visualize.py

These files perform the calculation of mapping every geocoded address to its nearest street segment and map a subset of those relations on a map of the region, respectively.




