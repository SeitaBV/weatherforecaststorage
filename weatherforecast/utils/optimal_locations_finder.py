import array
import random
from typing import List, Tuple

import numpy as np
from deap import algorithms
from deap import base
from deap import creator
from deap import tools
import pandas as pd

from weatherforecast.utils.haversine import haversine_distance_vec, haversine_distance


class OptimalLocationsFinder:

    def __init__(self, locations: pd.DataFrame, new_locations_size: int) -> None:
        self.locations = locations
        self.new_locations_size = new_locations_size
        # self.haversine_vec = np.vectorize(haversine_distance_vec, otypes=[np.float32])

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
        # mean_distance: float = selected_locations.groupby('location_name') \
        #     .apply(lambda location: pd.Series(
        #     self.haversine_vec(location.latitude, location.longitude, selected_locations.latitude,
        #                        selected_locations.longitude))).mean().mean()

        return mean_distance,

    def find_optimal_locations(self):
        IND_SIZE = self.new_locations_size

        if self.locations.shape[0] < IND_SIZE:
            return self.locations

        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", array.array, typecode='i', fitness=creator.FitnessMax)

        toolbox = base.Toolbox()

        # Attribute generator
        toolbox.register("indices", random.sample, range(self.locations.shape[0] - 1), IND_SIZE)

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

        algorithms.eaSimple(pop, toolbox, 0.3, 0.1, 15, stats=stats,
                            halloffame=hof)

        optimal_locations_indices = hof.items[0]

        return self.locations.iloc[optimal_locations_indices, :].copy()

    def create_locations_matrix(self, selected_locations):
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
