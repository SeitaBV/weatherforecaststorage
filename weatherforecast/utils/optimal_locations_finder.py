import array
import random
from typing import List, Tuple
import numpy as np
from deap import algorithms
from deap import base
from deap import creator
from deap import tools
import pandas as pd
from scipy.spatial.qhull import ConvexHull
from area import area
# import multiprocessing
from weatherforecast.utils.haversine import haversine_distance_vec


class OptimalLocationsFinder:

    def __init__(self, locations: pd.DataFrame, new_locations_size: int) -> None:
        self.locations = locations
        self.new_locations_size = new_locations_size

    def _eval_solution(self, individual: List[int]) -> Tuple[float]:
        if len(individual) == 1:
            return 0,

        if len(individual) > len(set(individual)):
            return -6371,

        selected_locations: pd.DataFrame = self.locations.iloc[individual, :].copy()
        locations_matrix = self.create_locations_matrix(selected_locations)
        distances = haversine_distance_vec(locations_matrix['latitude_1'], locations_matrix['longitude_1'],
                                           locations_matrix['latitude_2'], locations_matrix['longitude_2'])
        mean_distance = distances.median()

        if len(individual) > 4:
            area_sq_km = self.calculate_area(selected_locations)
            fitness = mean_distance + area_sq_km
        else:
            fitness = mean_distance

        return fitness,

    def find_optimal_locations(self):
        IND_SIZE = self.new_locations_size

        if self.locations.shape[0] < IND_SIZE:
            return self.locations

        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", array.array, typecode='i', fitness=creator.FitnessMax)

        toolbox = base.Toolbox()

        # pool = multiprocessing.Pool()
        # toolbox.register("map", pool.map)

        # Attribute generator
        toolbox.register("indices", random.sample, range(self.locations.shape[0]), IND_SIZE)

        # Structure initializers
        toolbox.register("individual", tools.initIterate, creator.Individual, toolbox.indices)
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)

        toolbox.register("mate", tools.cxTwoPoint)
        toolbox.register("mutate", tools.mutShuffleIndexes, indpb=0.1)
        toolbox.register("select", tools.selTournament, tournsize=5)
        toolbox.register("evaluate", self._eval_solution)

        # random.seed(169)

        pop = toolbox.population(n=500)

        hof = tools.HallOfFame(1)
        stats = tools.Statistics(lambda ind: ind.fitness.values)
        stats.register("avg", np.mean)
        stats.register("std", np.std)
        stats.register("min", np.min)
        stats.register("max", np.max)

        algorithms.eaSimple(pop, toolbox, 0.2, 0.4, 10, stats=stats,
                            halloffame=hof)

        optimal_locations_indices = hof.items[0]

        return self.locations.iloc[optimal_locations_indices, :].copy()

    @staticmethod
    def create_locations_matrix(selected_locations: pd.DataFrame) -> pd.DataFrame:
        locations = selected_locations.values
        data = []
        columns = ['location_1', 'longitude_1', 'latitude_1', 'location_2', 'longitude_2', 'latitude_2']
        for i in range(0, locations.shape[0] - 1):
            location_1 = locations[i]
            for j in range(i + 1, locations.shape[0]):
                location_2 = locations[j]
                data.append({
                    'location_1': location_1[2],
                    'longitude_1': location_1[1],
                    'latitude_1': location_1[0],
                    'location_2': location_2[2],
                    'longitude_2': location_2[1],
                    'latitude_2': location_2[0]
                })

        return pd.DataFrame(data=data, columns=columns)

    def calculate_area(self, selected_locations: pd.DataFrame) -> float:
        points = selected_locations.loc[:, ['longitude', 'latitude']].values
        hull = self.calculate_convex_hull(points)
        geojson_object = {'type': 'Polygon', 'coordinates': [hull.tolist()]}
        area_sq_km = area(geojson_object) / 1e+6
        return area_sq_km

    @staticmethod
    def calculate_convex_hull(points: np.ndarray) -> np.ndarray:
        hull = ConvexHull(points)
        hull_points = points[hull.vertices, :]
        return hull_points
