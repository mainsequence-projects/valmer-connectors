# syntax=docker/dockerfile:1
#
# MainSequence scaffold Dockerfile
# Base image resolved from DEFAULT_BASE_IMAGE or platform defaults.
#
FROM ghcr.io/main-sequence-server-side/poddeploymentorchestrator-jupyterhub-py311:latest

WORKDIR /app

# Copy the project into the image
COPY . /app

# NOTE:
# This is a scaffold. Your base image may already contain tooling.
# Customize as needed (install deps, set entrypoints, etc.).
