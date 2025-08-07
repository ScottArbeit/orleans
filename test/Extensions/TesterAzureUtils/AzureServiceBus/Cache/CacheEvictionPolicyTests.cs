using System;
using Microsoft.Extensions.Logging;
using Orleans.Streaming.AzureServiceBus.Cache;
using Xunit;

namespace Tester.AzureUtils.AzureServiceBus.Cache;

/// <summary>
/// Tests for cache eviction policy functionality.
/// </summary>
public class CacheEvictionPolicyTests
{
    [Fact]
    public void Constructor_WithValidParameters_CreatesPolicy()
    {
        // Act
        var policy = new CacheEvictionPolicy(
            strategy: CacheEvictionStrategy.LRU,
            messageTTL: TimeSpan.FromMinutes(30),
            maxIdleTime: TimeSpan.FromMinutes(10),
            maxCacheSize: 1000,
            maxMemorySizeBytes: 10 * 1024 * 1024,
            pressureThreshold: 0.8);

        // Assert
        Assert.Equal(CacheEvictionStrategy.LRU, policy.Strategy);
        Assert.Equal(TimeSpan.FromMinutes(30), policy.MessageTTL);
        Assert.Equal(TimeSpan.FromMinutes(10), policy.MaxIdleTime);
        Assert.Equal(1000, policy.MaxCacheSize);
        Assert.Equal(10 * 1024 * 1024, policy.MaxMemorySizeBytes);
        Assert.Equal(0.8, policy.PressureThreshold);
    }

    [Fact]
    public void ShouldEvictByAge_WhenMessageOlderThanTTL_ReturnsTrue()
    {
        // Arrange
        var policy = new CacheEvictionPolicy(messageTTL: TimeSpan.FromMinutes(10));
        var messageAge = TimeSpan.FromMinutes(15);

        // Act
        var shouldEvict = policy.ShouldEvictByAge(messageAge);

        // Assert
        Assert.True(shouldEvict);
    }

    [Fact]
    public void ShouldEvictByAge_WhenMessageNewerThanTTL_ReturnsFalse()
    {
        // Arrange
        var policy = new CacheEvictionPolicy(messageTTL: TimeSpan.FromMinutes(10));
        var messageAge = TimeSpan.FromMinutes(5);

        // Act
        var shouldEvict = policy.ShouldEvictByAge(messageAge);

        // Assert
        Assert.False(shouldEvict);
    }

    [Fact]
    public void ShouldEvictByIdleTime_WhenIdleTimeExceeded_ReturnsTrue()
    {
        // Arrange
        var policy = new CacheEvictionPolicy(maxIdleTime: TimeSpan.FromMinutes(5));
        var idleTime = TimeSpan.FromMinutes(10);

        // Act
        var shouldEvict = policy.ShouldEvictByIdleTime(idleTime);

        // Assert
        Assert.True(shouldEvict);
    }

    [Fact]
    public void IsCacheSizeExceeded_WhenSizeExceeded_ReturnsTrue()
    {
        // Arrange
        var policy = new CacheEvictionPolicy(maxCacheSize: 100);

        // Act
        var exceeded = policy.IsCacheSizeExceeded(150);

        // Assert
        Assert.True(exceeded);
    }

    [Fact]
    public void IsMemorySizeExceeded_WhenMemoryExceeded_ReturnsTrue()
    {
        // Arrange
        var policy = new CacheEvictionPolicy(maxMemorySizeBytes: 1024);

        // Act
        var exceeded = policy.IsMemorySizeExceeded(2048);

        // Assert
        Assert.True(exceeded);
    }

    [Fact]
    public void IsUnderPressure_WhenPressureExceedsThreshold_ReturnsTrue()
    {
        // Arrange
        var policy = new CacheEvictionPolicy(pressureThreshold: 0.8);

        // Act
        var underPressure = policy.IsUnderPressure(0.9);

        // Assert
        Assert.True(underPressure);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1.1)]
    public void Constructor_WithInvalidPressureThreshold_ThrowsArgumentOutOfRangeException(double invalidThreshold)
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => 
            new CacheEvictionPolicy(pressureThreshold: invalidThreshold));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-100)]
    public void Constructor_WithInvalidCacheSize_ThrowsArgumentOutOfRangeException(int invalidSize)
    {
        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => 
            new CacheEvictionPolicy(maxCacheSize: invalidSize));
    }
}