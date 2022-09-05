# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Visualisations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the in-built Visualisations

# COMMAND ----------

# Create a simple dataframe with a schema, and populate with data

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

my_schema = StructType([
  StructField('Make', StringType(), True),
  StructField('Model', StringType(), True),
  StructField('Engines', IntegerType(), True)
])

my_data = [('Airbus', 'A300', 2),
           ('Airbus', 'A310', 2),
           ('Airbus', 'A320', 2),
           ('Airbus', 'A330', 2),
           ('Airbus', 'A340', 4),
           ('Airbus', 'A350', 2),
           ('Airbus', 'A380', 4),
           ('Boeing', 'B707', 4),
           ('Boeing', 'B717', 2),
           ('Boeing', 'B727', 3),
           ('Boeing', 'B737', 2),
           ('Boeing', 'B747', 4),
           ('Boeing', 'B757', 2),
           ('Boeing', 'B767', 2),
           ('Boeing', 'B777', 2),
           ('Boeing', 'B787', 2)]

airliners = spark.createDataFrame(my_data, schema = my_schema)
display(airliners)

# COMMAND ----------

from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import Row
from decimal import Decimal

airlinesSchema = StructType(
  [
    StructField('Name', StringType()),
    StructField('Country', StringType()),
    StructField('age', IntegerType())
  ]
)

dfAirlines = spark.createDataFrame(
  [
    Row("American Airlines", "USA", 84),
    Row("British Airways", "GBR", 46),
    Row("Japan Airlines", "JPN", 69),
    Row("Singapore Airlines", "SGP", 47),
    Row("QANTAS", "AUS", 100)
  ], airlinesSchema
)

display(dfAirlines)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LaTeX support for mathematical equations
# MAGIC 
# MAGIC \\(y = \\sin({x}) \\)

# COMMAND ----------

from pyspark.sql.functions import expr, col, column

xDF = spark.range(-360, 360, 1).toDF('x')
sineDF = xDF.withColumn('y', expr('sin(radians(x))'))
display(sineDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3rd Party Libraries (eg Matplotlib)
# MAGIC #### Examples taken from matplotlib.org

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
import numpy as np
from mpl_toolkits.mplot3d import Axes3D

# COMMAND ----------

# Fixing random state for reproducibility
np.random.seed(19680801)

dt = 0.01
t = np.arange(0, 30, dt)
nse1 = np.random.randn(len(t))                 # white noise 1
nse2 = np.random.randn(len(t))                 # white noise 2

# Two signals with a coherent part at 10Hz and a random part
s1 = np.sin(2 * np.pi * 10 * t) + nse1
s2 = np.sin(2 * np.pi * 10 * t) + nse2

fig, axs = plt.subplots(2, 1)
axs[0].plot(t, s1, t, s2)
axs[0].set_xlim(0, 2)
axs[0].set_xlabel('time')
axs[0].set_ylabel('s1 and s2')
axs[0].grid(True)

cxy, f = axs[1].cohere(s1, s2, 256, 1. / dt)
axs[1].set_ylabel('coherence')

fig.tight_layout()
plt.show()

# COMMAND ----------

fig, ax = plt.subplots(subplot_kw={"projection": "3d"})

# Make data.
X = np.arange(-5, 5, 0.25)
Y = np.arange(-5, 5, 0.25)
X, Y = np.meshgrid(X, Y)
R = np.sqrt(X**2 + Y**2)
Z = np.sin(R)

# Plot the surface.
surf = ax.plot_surface(X, Y, Z, cmap=cm.coolwarm,
                       linewidth=0, antialiased=False)

# Customize the z axis.
ax.set_zlim(-1.01, 1.01)
ax.zaxis.set_major_locator(LinearLocator(10))
ax.zaxis.set_major_formatter(FormatStrFormatter('{x:.02f}'))

# Add a color bar which maps values to colors.
fig.colorbar(surf, shrink=0.5, aspect=5)

plt.show()

# COMMAND ----------

np.random.seed(19680801)


fig = plt.figure(figsize=(10,10))
ax = fig.add_subplot(111, projection='3d')

colors = ['r', 'g', 'b', 'y']
yticks = [3, 2, 1, 0]
for c, k in zip(colors, yticks):
    # Generate the random data for the y=k 'layer'.
    xs = np.arange(20)
    ys = np.random.rand(20)

    # You can provide either a single color or an array with the same length as
    # xs and ys. To demonstrate this, we color the first bar of each set cyan.
    cs = [c] * len(xs)
    cs[0] = 'c'

    # Plot the bar graph given by xs and ys on the plane y=k with 80% opacity.
    ax.bar(xs, ys, zs=k, zdir='y', color=cs, alpha=0.8)

ax.set_xlabel('X')
ax.set_ylabel('Y')
ax.set_zlabel('Z')

# On the y axis let's only label the discrete values that we have data for.
ax.set_yticks(yticks)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interactive Plots with Plotly

# COMMAND ----------

import plotly.graph_objects as go

import pandas as pd

# Read data from a csv
z_data = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/api_docs/mt_bruno_elevation.csv')

fig = go.Figure(data=[go.Surface(z=z_data.values)])

fig.update_layout(title='Mt Bruno Elevation', autosize=False,
                  width=500, height=500,
                  margin=dict(l=65, r=50, b=65, t=90))

fig.show()

# COMMAND ----------

import plotly.express as px
df = px.data.iris()
fig = px.scatter(df, x="sepal_width", y="sepal_length", color="species",
                 size='petal_length', hover_data=['petal_width'])
fig.show()

# COMMAND ----------


