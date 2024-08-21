#Create Email Notification Upon Transformation Error or Critical log

resource "aws_cloudwatch_log_metric_filter" "transform_error_detection" {
  name           = "transformErrorDetection"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.transform_log.name

  metric_transformation {
    name      = "transformErrorCount"
    namespace = "ApplicationMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "transform_critical_detection" {
  name           = "transformCriticalDetection"
  pattern        = "CRITICAL"
  log_group_name = aws_cloudwatch_log_group.transform_log.name

  metric_transformation {
    name      = "transformCriticalCount"
    namespace = "ApplicationMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_group" "transform_log" {
  name_prefix = "/aws/lambda/transform"

}

# Create a CloudWatch Alarm based on the Metric Filter
resource "aws_cloudwatch_metric_alarm" "transform_error_alarm" {
  alarm_name          = "transformErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "transformErrorCount"
  namespace           = "ApplicationMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Triggered when an ERROR is logged."
  alarm_actions       = [aws_sns_topic.alert_topic.arn]
}

resource "aws_cloudwatch_metric_alarm" "transform_critical_alarm" {
  alarm_name          = "transformCriticalAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "transformCriticalCount"
  namespace           = "ApplicationMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Triggered when an CRITICAL is logged."
  alarm_actions       = [aws_sns_topic.alert_topic2.arn]
}

# Create an SNS Topic for email notifications
resource "aws_sns_topic" "alert_topic" {
  name = "transform-error-alerts"
}

# Create an SNS Topic for email notifications
resource "aws_sns_topic" "alert_topic2" {
  name = "transform-critical-alerts"
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
