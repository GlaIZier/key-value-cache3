PATTERN = "%date{\"yyyy-MM-dd'T'HH:mm:ss,SSSXXX\"}] [%thread] %level %logger{50} - %msg%n"

appender("FILE", RollingFileAppender) {
    file = "logs/key-value-cache3.log"
    rollingPolicy(TimeBasedRollingPolicy) {
        FileNamePattern = "logs/key-value-cache3.%d{yyyy-MM-dd}.log"
        MaxHistory = 30
    }
    encoder(PatternLayoutEncoder) {
        pattern = PATTERN
    }
}

appender("CONSOLE", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = PATTERN
    }
}

logger("ru.glaizier.key.value.cache3.cache.strategy", DEBUG)

root(INFO, ["CONSOLE", "FILE"])