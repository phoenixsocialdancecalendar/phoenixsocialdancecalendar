import Foundation
import Vision
import AppKit

struct OCRLine {
    let x: Double
    let y: Double
    let width: Double
    let height: Double
    let text: String
}

func loadCGImage(at path: String) -> CGImage {
    let url = URL(fileURLWithPath: path)
    guard let image = NSImage(contentsOf: url) else {
        fatalError("Unable to load image at \(path)")
    }
    var rect = CGRect(origin: .zero, size: image.size)
    guard let cgImage = image.cgImage(forProposedRect: &rect, context: nil, hints: nil) else {
        fatalError("Unable to create CGImage for \(path)")
    }
    return cgImage
}

func recognizeText(in cgImage: CGImage) throws -> [OCRLine] {
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true
    request.recognitionLanguages = ["en-US"]
    request.minimumTextHeight = 0.01

    let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
    try handler.perform([request])

    return (request.results ?? []).compactMap { observation in
        guard let candidate = observation.topCandidates(1).first else {
            return nil
        }
        let normalized = candidate.string
            .replacingOccurrences(of: "\n", with: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if normalized.isEmpty {
            return nil
        }
        let box = observation.boundingBox
        return OCRLine(
            x: Double(box.origin.x),
            y: Double(box.origin.y),
            width: Double(box.size.width),
            height: Double(box.size.height),
            text: normalized
        )
    }
}

let arguments = CommandLine.arguments
guard arguments.count >= 2 else {
    fatalError("Usage: swift ocr_cdc_calendar.swift /path/to/image")
}

let imagePath = arguments[1]
let cgImage = loadCGImage(at: imagePath)
let lines = try recognizeText(in: cgImage)

for line in lines {
    print(String(format: "%.4f|%.4f|%.4f|%.4f|%@", line.x, line.y, line.width, line.height, line.text))
}
