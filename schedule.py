scheduler for monthly daily weekly biweekly quaterly and  return 
the start datetime and endatetime using python
Here's a Python scheduler that handles daily, weekly, biweekly, monthly, and quarterly tasks, returning both start and end datetimes for each occurrence:

python
from datetime import datetime, timedelta
from typing import Generator, Tuple, Optional

def schedule_daily(
    start_date: datetime,
    end_date: datetime,
    task_duration: timedelta = timedelta(hours=1),
    time_of_day: Optional[Tuple[int, int]] = None
) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    Schedule a daily task between start_date and end_date.
    
    Args:
        start_date: First possible date for the task
        end_date: Last possible date for the task
        task_duration: Duration of the task
        time_of_day: Optional (hour, minute) to fix the time each day
    
    Yields:
        Tuple of (start_datetime, end_datetime) for each occurrence
    """
    current = start_date
    if time_of_day:
        current = current.replace(hour=time_of_day[0], minute=time_of_day[1], second=0, microsecond=0)
        if current < start_date:
            current += timedelta(days=1)
    
    while current <= end_date:
        yield (current, current + task_duration)
        current = current.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        if time_of_day:
            current = current.replace(hour=time_of_day[0], minute=time_of_day[1])


def schedule_weekly(
    start_date: datetime,
    end_date: datetime,
    weekday: int,
    task_duration: timedelta = timedelta(hours=1),
    time_of_day: Optional[Tuple[int, int]] = None
) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    Schedule a weekly task on a specific weekday between start_date and end_date.
    
    Args:
        start_date: First possible date for the task
        end_date: Last possible date for the task
        weekday: 0=Monday, 6=Sunday
        task_duration: Duration of the task
        time_of_day: Optional (hour, minute) to fix the time each day
    
    Yields:
        Tuple of (start_datetime, end_datetime) for each occurrence
    """
    current = start_date
    days_ahead = (weekday - current.weekday()) % 7
    current += timedelta(days=days_ahead)
    if time_of_day:
        current = current.replace(hour=time_of_day[0], minute=time_of_day[1], second=0, microsecond=0)
        if current < start_date:
            current += timedelta(weeks=1)
    
    while current <= end_date:
        yield (current, current + task_duration)
        current += timedelta(weeks=1)
        if time_of_day:
            current = current.replace(hour=time_of_day[0], minute=time_of_day[1])


def schedule_biweekly(
    start_date: datetime,
    end_date: datetime,
    weekday: int,
    task_duration: timedelta = timedelta(hours=1),
    time_of_day: Optional[Tuple[int, int]] = None
) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    Schedule a biweekly (every 2 weeks) task on a specific weekday between start_date and end_date.
    
    Args:
        start_date: First possible date for the task
        end_date: Last possible date for the task
        weekday: 0=Monday, 6=Sunday
        task_duration: Duration of the task
        time_of_day: Optional (hour, minute) to fix the time each day
    
    Yields:
        Tuple of (start_datetime, end_datetime) for each occurrence
    """
    current = start_date
    days_ahead = (weekday - current.weekday()) % 7
    current += timedelta(days=days_ahead)
    if time_of_day:
        current = current.replace(hour=time_of_day[0], minute=time_of_day[1], second=0, microsecond=0)
        if current < start_date:
            current += timedelta(weeks=2)
    
    while current <= end_date:
        yield (current, current + task_duration)
        current += timedelta(weeks=2)
        if time_of_day:
            current = current.replace(hour=time_of_day[0], minute=time_of_day[1])


def schedule_monthly(
    start_date: datetime,
    end_date: datetime,
    day_of_month: int,
    task_duration: timedelta = timedelta(hours=1),
    time_of_day: Optional[Tuple[int, int]] = None
) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    Schedule a monthly task on a specific day of month between start_date and end_date.
    
    Args:
        start_date: First possible date for the task
        end_date: Last possible date for the task
        day_of_month: Day of month (1-31)
        task_duration: Duration of the task
        time_of_day: Optional (hour, minute) to fix the time each day
    
    Yields:
        Tuple of (start_datetime, end_datetime) for each occurrence
    """
    current = start_date.replace(day=1)
    # Find first valid occurrence on or after start_date
    while True:
        try:
            candidate = current.replace(day=day_of_month)
            if time_of_day:
                candidate = candidate.replace(hour=time_of_day[0], minute=time_of_day[1], second=0, microsecond=0)
            
            if candidate >= start_date:
                current = candidate
                break
        except ValueError:
            pass  # Day doesn't exist in this month
        
        # Move to next month
        next_month = current.month % 12 + 1
        next_year = current.year + (1 if next_month == 1 else 0)
        current = current.replace(year=next_year, month=next_month, day=1)
    
    while current <= end_date:
        yield (current, current + task_duration)
        
        # Move to next month
        next_month = current.month % 12 + 1
        next_year = current.year + (1 if next_month == 1 else 0)
        try:
            current = current.replace(year=next_year, month=next_month, day=day_of_month)
            if time_of_day:
                current = current.replace(hour=time_of_day[0], minute=time_of_day[1])
        except ValueError:
            # Day doesn't exist in next month, use last day
            current = current.replace(year=next_year, month=next_month, day=1) + timedelta(days=32)
            current = current.replace(day=1) - timedelta(days=1)
            if time_of_day:
                current = current.replace(hour=time_of_day[0], minute=time_of_day[1])


def schedule_quarterly(
    start_date: datetime,
    end_date: datetime,
    day_of_quarter: int,
    task_duration: timedelta = timedelta(hours=1),
    time_of_day: Optional[Tuple[int, int]] = None,
    quarter_months: Tuple[int, int, int, int] = (1, 4, 7, 10)
) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    Schedule a quarterly task on a specific day of quarter between start_date and end_date.
    
    Args:
        start_date: First possible date for the task
        end_date: Last possible date for the task
        day_of_quarter: Day of quarter (1-92)
        task_duration: Duration of the task
        time_of_day: Optional (hour, minute) to fix the time each day
        quarter_months: Months that start each quarter (default: Jan, Apr, Jul, Oct)
    
    Yields:
        Tuple of (start_datetime, end_datetime) for each occurrence
    """
    current = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Find first valid occurrence on or after start_date
    while True:
        # Find current quarter
        for month in sorted(quarter_months):
            if current.month <= month:
                quarter_start = current.replace(month=month, day=1)
                break
        else:
            quarter_start = current.replace(year=current.year + 1, month=quarter_months[0], day=1)
        
        candidate = quarter_start + timedelta(days=day_of_quarter - 1)
        if time_of_day:
            candidate = candidate.replace(hour=time_of_day[0], minute=time_of_day[1], second=0, microsecond=0)
        
        if candidate >= start_date and candidate.month in [quarter_months[0], quarter_months[0]+1, quarter_months[0]+2]:
            current = candidate
            break
        
        # Move to next quarter
        next_quarter_index = (sorted(quarter_months).index(quarter_start.month) + 1
        if next_quarter_index >= len(quarter_months):
            next_quarter_index = 0
            next_year = quarter_start.year + 1
        else:
            next_year = quarter_start.year
        next_month = sorted(quarter_months)[next_quarter_index]
        current = current.replace(year=next_year, month=next_month, day=1)
    
    while current <= end_date:
        yield (current, current + task_duration)
        
        # Move to next quarter
        current_quarter_month = current.month
        for i, month in enumerate(sorted(quarter_months)):
            if current_quarter_month <= month:
                current_quarter_month = month
                break
        
        next_quarter_index = (sorted(quarter_months).index(current_quarter_month) + 1) % len(quarter_months)
        next_year = current.year + (1 if next_quarter_index == 0 else 0)
        next_month = sorted(quarter_months)[next_quarter_index]
        
        quarter_start = current.replace(year=next_year, month=next_month, day=1)
        candidate = quarter_start + timedelta(days=day_of_quarter - 1)
        if time_of_day:
            candidate = candidate.replace(hour=time_of_day[0], minute=time_of_day[1])
        
        # Handle case where day_of_quarter exceeds quarter length
        next_quarter_end = (quarter_start.replace(month=next_month+3, day=1) - timedelta(days=1))
        current = min(candidate, next_quarter_end)


# Example Usage
if __name__ == "__main__":
    print("Daily schedule (9 AM for 1 hour):")
    for start, end in schedule_daily(
        datetime(2023, 1, 1),
        datetime(2023, 1, 5),
        time_of_day=(9, 0)
    ):
        print(f"{start} - {end}")
    
    print("\nWeekly schedule (Every Monday at 2 PM for 2 hours):")
    for start, end in schedule_weekly(
        datetime(2023, 1, 1),
        datetime(2023, 2, 1),
        weekday=0,  # Monday
        task_duration=timedelta(hours=2),
        time_of_day=(14, 0)
    ):
        print(f"{start} - {end}")
    
    print("\nBiweekly schedule (Every other Wednesday at 10:30 AM for 30 mins):")
    for start, end in schedule_biweekly(
        datetime(2023, 1, 1),
        datetime(2023, 4, 1),
        weekday=2,  # Wednesday
        task_duration=timedelta(minutes=30),
        time_of_day=(10, 30)
    ):
        print(f"{start} - {end}")
    
    print("\nMonthly schedule (15th of each month at 3 PM for 1 hour):")
    for start, end in schedule_monthly(
        datetime(2023, 1, 1),
        datetime(2023, 6, 1),
        day_of_month=15,
        time_of_day=(15, 0)
    ):
        print(f"{start} - {end}")
    
    print("\nQuarterly schedule (50th day of quarter at 9 AM for 3 hours):")
    for start, end in schedule_quarterly(
        datetime(2023, 1, 1),
        datetime(2024, 1, 1),
        day_of_quarter=50,
        task_duration=timedelta(hours=3),
        time_of_day=(9, 0)
    ):
        print(f"{start} - {end}")
Key Features:
Flexible Scheduling:

Daily: Specify optional time of day

Weekly: Choose weekday (0=Monday to 6=Sunday)

Biweekly: Every 2 weeks on specified weekday

Monthly: Specific day of month (handles months with fewer days)

Quarterly: Specific day of quarter (customizable quarter months)

Duration Handling:

Each function returns both start and end datetimes

Default duration is 1 hour but can be customized

Time Specification:

Optional time_of_day parameter (hour, minute) to fix the time

Robust Date Handling:

Properly handles month boundaries

Adjusts for invalid dates (e.g., February 30th)

Ensures dates fall within specified ranges

Type Hints:

Uses Python type hints for better code clarity

This implementation provides a comprehensive solution for recurring task scheduling with precise datetime control.
