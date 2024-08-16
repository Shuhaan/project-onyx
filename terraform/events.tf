resource "aws_cloudwatch_event_rule" "extract_scheduler" {
  name                = "extract_lambda_scheduler"
  description         = "Invoke extract lambda function every 5 mins and upload results to CloudWatch"
  schedule_expression = "rate(10 minutes)"
  depends_on          = [aws_lambda_function.extract_handler]
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.extract_scheduler.name
  target_id = "extract_lambda_target"
  arn       = aws_lambda_function.extract_handler.arn
}

resource "aws_lambda_permission" "extract_permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.extract_scheduler.arn
}

#Create Email Notification Upon Extraction Error

resource "aws_cloudwatch_log_metric_filter" "extract_error_detection" {
  name           = "ExtractErrorDetection"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.extract_log.name

  metric_transformation {
    name      = "ExtractErrorCount"
    namespace = "ApplicationMetrics"
    value     = "1"
  }

  depends_on = [ aws_iam_role.extract_lambda_role ]
}
resource "aws_cloudwatch_log_group" "extract_log" {
  name_prefix = "/aws/lambda/extract"
  
}
# Create an SNS Topic for email notifications
resource "aws_sns_topic" "alert_topic" {
  name = "extract-error-alerts"
}

# Subscribe the email address to the SNS Topic
resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.alert_topic.arn
  protocol  = "email"
  endpoint  = "shuhaan15@hotmail.com"
  depends_on = [ aws_sns_topic.alert_topic ]
}

# Create a CloudWatch Alarm based on the Metric Filter
resource "aws_cloudwatch_metric_alarm" "extract_error_alarm" {
  alarm_name          = "ExtractErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ExtractErrorCount"
  namespace           = "ApplicationMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Triggered when an ERROR is logged."

  alarm_actions = [aws_sns_topic.alert_topic.arn]
}