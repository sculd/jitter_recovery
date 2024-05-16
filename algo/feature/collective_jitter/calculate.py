
default_window = 40
default_collective_window = 40


class CollectiveJitterFeatureParam:
    def __init__(self, window: int, collective_window: int):
        self.window = window
        self.collective_window = collective_window

    @staticmethod
    def get_default_param():
        return CollectiveJitterFeatureParam(
            default_window, default_collective_window)

    def __str__(self):
        return ', '.join([f'{k}: {str(v)}' for k, v in vars(self).items()])

