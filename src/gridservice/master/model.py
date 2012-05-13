from gridservice.master.grid import Grid
from gridservice.master.scheduler import BullshitScheduler

# Bring the Grid online
grid = Grid(BullshitScheduler)
