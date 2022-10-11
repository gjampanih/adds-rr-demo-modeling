# adds-rr-demo-modeling

## Model Overview
Research response (rr) model analyzes and projects how well a song will perform on a weekly basis at any 
given station , for that stations' population at large. Further analysis 
would be needed to understand how songs are resonating with sub-populations.

The core input component to the rr model is listener surveys called callout/OMT research performed on a 
weekly at stations designated as callout stations. 
Callout/OMT research surveys are stratified by various demographic subgroups 
(such as Old/Young, Male/Female etc) but, up to this point, we have not tried to stratify scoring by demographics.

The rr-demo model in this repository extends the rr model to be able to model and quantify differences in response across different demographic subgroups **including but not limited to: 
- Age: Old vs Young
- Race: Black vs Hispanic vs White/Asian/Other
- Gender (certain formats only)
- Listenership: Core vs Cume.

The rr-demo model is a collection of regression models which capture scores at the demographic level and analyze historic data to identify "wobbles" vs actual shifts in listener sentiment.
Since business stakeholders are primarily interested in differences in responses across demographic groups the insights
presented are refined into score gaps between pairs of related demographic groups as opposed to presenting individual scores for each demographic group.

## Data
Spins Data (Source: Mediabase)
The spins information provided by Mediabase is aggregated weekly by song-station, song-format, song-market. Statistics 
computed include average, most-recent, minimum and maximum spins.
<details><summary>Open to view columns </summary>

|Feature|Format|Type|Description|
|---|---|---|---|
|**Id**|*integer*|Nominal|Identifier for each property.|
|**PID**|*integer*|Nominal|Parcel identification number - can be usedwith city web site for parcel review.|
|**MS SubClass**|*integer*|Nominal|Identifies the type of dwellinginvolved in the sale. Type is coded, please refer to full datadocumentation|
|**MS Zoning**|*string*|Nominal|Identifies the general zoningclassification of the sale.|
</details>

Callout/OMT Research (Source: CMM)
The callout research information provided by Mediabase is aggregated weekly by song-station-demographic. Statistics 
computed include average, most-recent, minimum, maximum, and 4/8 week moving/smoothed averages.
<details><summary>Open to view columns </summary>

|Feature|Format|Type|Description|
|---|---|---|---|
|**Id**|*integer*|Nominal|Identifier for each property.|
|**PID**|*integer*|Nominal|Parcel identification number - can be usedwith city web site for parcel review.|
|**MS SubClass**|*integer*|Nominal|Identifies the type of dwellinginvolved in the sale. Type is coded, please refer to full datadocumentation|
|**MS Zoning**|*string*|Nominal|Identifies the general zoningclassification of the sale.|
</details>


## Criterion for Scoring Eligibility
At least two format callout reports at 150 spins or higher

At least two station callout reports at 150 spins or higher

Atleast 500 station spins as of date when song is being scored

## Modelling 
### Training Data
Data from the two years into the past from the score date is used as training data subject to the availability of TAA 
quintile information (TAA quintiles was introduced in Nov. 2021)

### Loss Function (for estimation)

Gradient Boosted Quantile Regression for Lower Threshold for Wobbles : Pinball/Quantile loss with $\alpha =0.05$

Gradient Boosted Quantile Regression for Wobbles : Pinball/Quantile loss with $\alpha =0.95$

Gradient Boosted Regressor for TAA by Demographic Prediction:  Squared Error

NOTE: Pinball Loss, $pinball(y, \hat{y})$ is defined as 
$$pinball(y, \hat{y}) = \frac{1}{n_{samples}}\sum\limits_{i=0}^{n-1} \alpha(max(y - \hat{y}, 0)) + (1- \alpha)(max(\hat{y} - y, 0))$$

### Evaluation Metric
Gradient Boosted Quantile Regression for Lower Threshold for Wobbles : Pinball/Quantile loss with $\alpha =0.05$

Gradient Boosted Quantile Regression for Wobbles : Pinball/Quantile loss with $\alpha =0.95$

Gradient Boosted Regressor for TAA by Demographic Prediction:  Mean Absolute Percentage Error (MAPE).
MAPE is chosen over Mean Squared Error (MSE) for greater interpretability.
MAPE is chosen over Mean Absolute Error (MAE) to normalize for the 
magnitude of the absolute error across different pop score ranges
### Cross validation
Two-fold cross validation for each of the quantile regression models and the TAA by demographic regressor.

Pinball loss at appropriate alpha is used for the quantile regression models for detecting wobbles
### Testing Data
For model calibration a test data set of 8 weeks of songs with callout research was used.

### Model refresh Cadence
Models are re-trained every 8 weeks to account for data drift.

## Reporting and Visualization
Score gaps between pairs of demographic groups are reported (for instance, Hispanic
listeners scored 10% higher than White listeners for song XYZ)

Smoothed pop score visualizations are generated for songs with sufficient data to facililiate
smoothing. Similar-station/National level aggregates are used to impute missing values for smoothing.
