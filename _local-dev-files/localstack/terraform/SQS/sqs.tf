variable "global_name" {}
variable "tags" {}


provider "aws" {
  region     = "ap-northeast-1"
}

resource "aws_sqs_queue" "device_healthy_check_queue" {
  name = "${var.global_name}-sqs-q-monosec-device-healthy-check"
  message_retention_seconds = 60 * 60 * 24 * 4
  visibility_timeout_seconds = 335
  receive_wait_time_seconds = 5

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.device_healthy_check_queue_deadletter.arn
    maxReceiveCount     = 4
  })
  tags      = var.tags
}

resource "aws_sqs_queue" "device_healthy_check_queue_deadletter" {
  name = "${var.global_name}-sqs-q-monosec-device-healthy-check-dlq"
  tags      = var.tags
}

resource "aws_sqs_queue_redrive_allow_policy" "device_healthy_queue_redrive_allow_policy" {
  queue_url = aws_sqs_queue.device_healthy_check_queue_deadletter.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue",
    sourceQueueArns   = [aws_sqs_queue.device_healthy_check_queue.arn]
  })
}

resource "aws_sqs_queue" "schedule-control_queue" {
  name = "${var.global_name}-sqsq-schedule-control"
  message_retention_seconds = 60 * 60 * 24 * 4
  visibility_timeout_seconds = 335
  receive_wait_time_seconds = 5

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.schedule-control_queue_deadletter.arn
    maxReceiveCount     = 4
  })
  tags      = var.tags
}

resource "aws_sqs_queue" "schedule-control_queue_deadletter" {
  name = "${var.global_name}-sqsq-schedule-control-dlq"
  tags      = var.tags
}

resource "aws_sqs_queue_redrive_allow_policy" "schedule-control_queue_redrive_allow_policy" {
  queue_url = aws_sqs_queue.schedule-control_queue_deadletter.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue",
    sourceQueueArns   = [aws_sqs_queue.schedule-control_queue.arn]
  })
}