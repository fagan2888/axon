import logging

from schedule_matcher_bot import ScheduleMatchingBot


def main():
    logging.debug('[start] Schedule-matching bot.')
    schedule_matching_bot = ScheduleMatchingBot()
    schedule_matching_bot.start()

if __name__ == '__main__':
    main()