import pickle
import matplotlib.pyplot as plt


data: dict[str, list[float]] = {}

with open(file="file.pickle", mode="rb") as fp:
    data = pickle.load(file=fp)
    fp.close()

print(data)

plt.boxplot(x=data["time"])
plt.savefig("test.png")
