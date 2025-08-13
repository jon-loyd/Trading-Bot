import matplotlib.dates as mdates
import matplotlib.pyplot as plt

def plot_hlc(df):
    """Plot high, low, and close prices"""
    plt.figure(figsize=(10, 5))
    plt.plot(df['timestamp'], df['close'], label='Close Price')
    plt.plot(df['timestamp'], df['low'], label='Low Price')
    plt.plot(df['timestamp'], df['high'], label='High Price')

    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('Stock Prices Over Time')

    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d-%y'))
    plt.xticks(rotation=45)

    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_moving_averages(df):
    """Plot closing prices against 50 and 200 day moving averages"""
    plt.figure(figsize=(10, 5))
    plt.plot(df['timestamp'], df['close'], label='Close Price')
    plt.plot(df['timestamp'], df['ma50'], label='50-Day MA')
    plt.plot(df['timestamp'], df['ma200'], label='200-Day MA')

    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('Stock Prices Over Time')

    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d-%y'))
    plt.xticks(rotation=45)

    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
