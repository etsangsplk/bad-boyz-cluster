from gridservice.grid import Grid, RoundRobinScheduler

# Bring the Grid online

scheduler = RoundRobinScheduler()
grid = Grid(scheduler)
