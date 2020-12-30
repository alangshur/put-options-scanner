from src.executor.loop import LoopMonitorExecutor


if __name__ == '__main__':

    # execute loop scan
    executor = LoopMonitorExecutor(
        delay_secs=900, 
        repeat_window=3600, 
        score_threshold=40
    )
    executor.run()
