# Use an official Node.js runtime as a parent image
FROM node:18-alpine

# Install FFmpeg
RUN apk add --no-cache ffmpeg

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy package files from the controller directory first to leverage caching
COPY controller/package*.json ./

# Install dependencies
RUN npm install

# Copy the controller and view directories into the image
# The controller's content goes to the root of WORKDIR
# The view directory is created inside WORKDIR
COPY controller/ .
COPY view/ ./view/

# Create temp directory
RUN mkdir -p /tmp/torrent-streams

# Your app binds to port 6543, so we expose it
EXPOSE 6543

# Define the command to run your app
CMD [ "node", "index.js" ]