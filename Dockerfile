# syntax=docker/dockerfile:1
#
# MainSequence scaffold Dockerfile
# Base image resolved from DEFAULT_BASE_IMAGE or platform defaults.
#
FROM ghcr.io/main-sequence-server-side/poddeploymentorchestrator-python-py313@sha256:8b2477adbc42fcd0e754ea853bf3913be5e3e8bb142e9d9dff58cfa940e75382

WORKDIR /app

# Copy the project into the image
COPY . /app

# NOTE:
# This is a scaffold. Your base image may already contain tooling.
# Customize as needed (install deps, set entrypoints, etc.).
