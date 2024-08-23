#Create Email Notification Upon Load Error or Critical log

resource "aws_cloudwatch_log_metric_filter" "load_error_detection" {
  name           = "LoadErrorDetection"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.load_log.name

  metric_transformation {
    name      = "LoadErrorCount"
    namespace = "ApplicationMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "load_critical_detection" {
  name           = "LoadCriticalDetection"
  pattern        = "CRITICAL"
  log_group_name = aws_cloudwatch_log_group.load_log.name

  metric_transformation {
    name      = "LoadCriticalCount"
    namespace = "ApplicationMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_group" "load_log" {
  name_prefix = "/aws/lambda/load"

}

# Create a CloudWatch Alarm based on the Metric Filter
resource "aws_cloudwatch_metric_alarm" "load_error_alarm" {
  alarm_name          = "LoadErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "LoadErrorCount"
  namespace           = "ApplicationMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Triggered when an ERROR is logged."
  alarm_actions       = [aws_sns_topic.alert_topic.arn]
}

resource "aws_cloudwatch_metric_alarm" "load_critical_alarm" {
  alarm_name          = "LoadCriticalAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "LoadCriticalCount"
  namespace           = "ApplicationMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Triggered when an CRITICAL is logged."
  alarm_actions       = [aws_sns_topic.alert_topic2.arn]
}

# Create an SNS Topic for email notifications
resource "aws_sns_topic" "alert_topic" {
  name = "load-error-alerts"
}

# Create an SNS Topic for email notifications
resource "aws_sns_topic" "alert_topic2" {
  name = "load-critical-alerts"
}

# Subscribe the email address to the SNS Topic
resource "aws_sns_topic_subscription" "email_subscription" {
  for_each   = toset(["shuhaan15@hotmail.com", "Arif@SyedUK.com", "hasanbmalik@gmail.com", "ewanritchie@hotmail.co.uk", "ayubmensah4@gmail.com", "mail@saifuddin.uk"])
  topic_arn  = aws_sns_topic.alert_topic.arn
  protocol   = "email"
  endpoint   = each.value
  depends_on = [aws_sns_topic.alert_topic]
}

resource "aws_sns_topic_subscription" "email_subscription2" {
  for_each   = toset(["shuhaan15@hotmail.com", "Arif@SyedUK.com", "hasanbmalik@gmail.com", "ewanritchie@hotmail.co.uk", "ayubmensah4@gmail.com", "mail@saifuddin.uk"])
  topic_arn  = aws_sns_topic.alert_topic2.arn
  protocol   = "email"
  endpoint   = each.value
  depends_on = [aws_sns_topic.alert_topic2]
}
