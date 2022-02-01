package software.amazon.kafkaconnect.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafkaconnect.model.BadRequestException;
import software.amazon.awssdk.services.kafkaconnect.model.ConflictException;
import software.amazon.awssdk.services.kafkaconnect.model.InternalServerErrorException;
import software.amazon.awssdk.services.kafkaconnect.model.NotFoundException;
import software.amazon.awssdk.services.kafkaconnect.model.UnauthorizedException;
import software.amazon.cloudformation.exceptions.BaseHandlerException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class ExceptionTranslatorTest {
    private static final String TEST_IDENTIFIER = "connector-test-name";
    private static final String TEST_MESSAGE = "test-message";
    private static final String TEST_INVALID_PARAMETER = "test-parameter";
    private final ExceptionTranslator exceptionTranslator = new ExceptionTranslator();

    @Test
    public void translateToCfnException_NotFoundException_MapsToCfnNotFoundException() {
        final NotFoundException exception = NotFoundException.builder()
            .message(TEST_MESSAGE)
            .build();

        runTranslateToCfnExceptionAndVerifyOutput(exception, CfnNotFoundException.class,
            "Resource of type 'AWS::KafkaConnect::Connector' with identifier 'connector-test-name' was not found.");
    }

    @Test
    public void translateToCfnException_BadRequestException_MapsToCfnInvalidRequestException() {
        final BadRequestException exception = BadRequestException.builder()
            .message(TEST_MESSAGE)
            .build();

        runTranslateToCfnExceptionAndVerifyOutput(exception, CfnInvalidRequestException.class,
            "Invalid request provided: AWS::KafkaConnect::Connector");
    }

    @Test
    public void translateToCfnException_ConflictException_MapsToCfnAlreadyExistsException() {
        final ConflictException exception = ConflictException.builder()
            .message(TEST_MESSAGE)
            .build();

        runTranslateToCfnExceptionAndVerifyOutput(exception, CfnAlreadyExistsException.class,
            "Resource of type 'AWS::KafkaConnect::Connector' with identifier 'connector-test-name' " +
                "already exists.");
    }

    @Test
    public void translateToCfnException_InternalServerErrorException_MapsToCfnInternalFailureException() {
        final InternalServerErrorException exception = InternalServerErrorException.builder()
            .message(TEST_MESSAGE)
            .build();

        runTranslateToCfnExceptionAndVerifyOutput(exception, CfnInternalFailureException.class,
            "Internal error occurred.");
    }

    @Test
    public void translateToCfnException_UnauthorizedException_MapsToCfnAccessDeniedException() {
        final UnauthorizedException exception = UnauthorizedException.builder()
            .message(TEST_MESSAGE)
            .build();

        runTranslateToCfnExceptionAndVerifyOutput(exception, CfnAccessDeniedException.class,
            "Access denied for operation 'AWS::KafkaConnect::Connector'.");
    }

    @Test
    public void translateToCfnException_Other_MapsToCfnGeneralServiceException() {
        final AwsServiceException exception = AwsServiceException.builder()
            .message(TEST_MESSAGE)
            .build();

        runTranslateToCfnExceptionAndVerifyOutput(exception, CfnGeneralServiceException.class,
            "Error occurred during operation '" + TEST_MESSAGE + "'.");
    }

    private void runTranslateToCfnExceptionAndVerifyOutput(final AwsServiceException exception,
        final Class<? extends BaseHandlerException> expectedExceptionClass, final String expectedMessage) {

        final BaseHandlerException result = exceptionTranslator.translateToCfnException(exception, TEST_IDENTIFIER);

        assertThat(result.getClass()).isEqualTo(expectedExceptionClass);
        assertThat(result.getMessage()).isEqualTo(expectedMessage);
    }
}
