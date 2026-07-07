import mongoose from "mongoose";
import cloudinary from "../lib/cloudinary.js";
import Message from "../models/message.js";
import { User } from "../models/User.js";
import { publishEvent } from "../kafka/producer.js";
import { TOPICS } from "../kafka/topics.js";

export const getAllContacts=async (req,res)=>{
    try {
        const loggedInuser=req.user._id
        const filterUserById=await User.find({_id:{ $ne:loggedInuser}}).select("-password")

        res.status(200).json(filterUserById)

    } catch (error) {
        console.log("Error in Getting Contacts",error)
        return res.status(500).json({mesaage:"Internal Server Error"})
    }
}

export const getAllChatByUserId=async (req,res)=>{
    try {
        const myId=req.user._id
        const {id:UserToChat}=req.params

        const message=await Message.find({
            $or:[
                {senderId:myId,receiverId:UserToChat},
                {senderId:UserToChat,receiverId:myId}
            ]
        })

        res.status(200).json(message)
    } catch (error) {
        console.log("Not able to get chats",error)
        res.status(500).json({message:"Internal Server Error"})
    }
}

export const sendMessage=async (req,res)=>{
    const {image,text}=req.body
    const senderId=req.user._id
    const {id:receiverId}=req.params
    if (!text && !image) {
      return res.status(400).json({ message: "Text or image is required." });
    }
    if (senderId.equals(receiverId)) {
      return res.status(400).json({ message: "Cannot send messages to yourself." });
    }
    const receiverExists = await User.exists({ _id: receiverId });
    if (!receiverExists) {
      return res.status(404).json({ message: "Receiver not found." });
    }
    let imageUrl
    if(image)
    {
        const uploadResponse=await cloudinary.uploader.upload(image)
        imageUrl=uploadResponse.secure_url
    }

    // Generate the id up front so we can hand it back to the caller
    // immediately, before the message has actually been persisted.
    const messageId = new mongoose.Types.ObjectId()
    const createdAt = new Date()

    const messagePayload = {
        _id: messageId,
        senderId,
        receiverId,
        text,
        image: imageUrl,
        createdAt,
    }

    // Instead of writing to Mongo + emitting on the socket directly from
    // the request handler, we drop the event on Kafka and return right
    // away. A separate worker pool (src/worker.js) consumes the topic,
    // persists the message, and delivers it in real time. This means:
    //  - a burst of messages gets buffered by Kafka instead of hammering
    //    Mongo or blocking this API instance
    //  - message persistence/delivery can be scaled (more worker replicas
    //    or partitions) completely independently of the HTTP/API tier
    //  - keying by the conversation keeps message order intact per chat
    const conversationKey = [senderId.toString(), receiverId.toString()].sort().join(":")
    await publishEvent(TOPICS.CHAT_MESSAGES, conversationKey, messagePayload)

    // 202 Accepted: the message is queued, not yet guaranteed persisted.
    // The client will receive the real-time "newMessage" socket event once
    // the worker has actually saved it (typically milliseconds later).
    return res.status(202).json(messagePayload)
}


export const getChatPartener=async (req,res)=>{

    try {
        const loggedInuser=req.user._id
    
        const mesaage=await Message.find({
            $or:[
                {senderId:loggedInuser},{receiverId:loggedInuser}],
        })
    
        const chatPartenerId= [...new Set(mesaage.map((msg) => msg.senderId.toString()===loggedInuser.toString()?msg.receiverId.toString():msg.senderId.toString()))]
    
        const chatParteners=await User.find({_id:{$in:chatPartenerId}}).select("-password")
    
        return res.status(200).json(chatParteners)
    } catch (error) {
        console.log("Error in getChat Partener controller",error)
        return res.status(500).json({mesaage:"Interenalserver error"})
    }

}